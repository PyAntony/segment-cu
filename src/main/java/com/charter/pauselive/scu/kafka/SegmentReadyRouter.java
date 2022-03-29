package com.charter.pauselive.scu.kafka;

import com.charter.pauselive.scu.model.ReadyMeta;
import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.vavr.collection.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

@ApplicationScoped
public class SegmentReadyRouter {
    @ConfigProperty(name = "readyrouter.segmentready.topic")
    String segmentReadyTopic;
    @ConfigProperty(name = "%prod.kafka.bootstrap.servers")
    String kafkaBrokers;
    @ConfigProperty(name = "readyrouter.poll.duration.milli")
    int pollMaxDuration;
    @ConfigProperty(name = "readyrouter.consumers.num")
    int consumersNum;

    private LinkedBlockingQueue<KafkaConsumer<String, byte[]>> seekers;
    String subscribedChannel = "copy-ready-topic";

    @Inject
    KafkaClientService clientService;

    @Inject
    void setQueue() {
        seekers = new LinkedBlockingQueue<>(consumersNum);

        var topicPartitions = getAllPartitions(
            clientService.getConsumer(subscribedChannel).unwrap(),
            segmentReadyTopic
        );

        Log.debugf("TopicPartitions found for seekers: %s", topicPartitions);
        List.range(0, consumersNum).forEach(__ -> {
            var consumer = getNewSeeker();
            consumer.assign(topicPartitions.toJavaList());
            seekers.offer(consumer);
        });
    }

    @Inject
    @Channel("segment-ready-router")
    Emitter<ReadyMeta> kafkaSeekEmitter;

    public void seekAndFetch(ReadyMeta loc) {
        kafkaSeekEmitter.send(loc);
    }

    private byte[] fetchCopyReadyMessage(String topic, ReadyMeta location) {
        Log.debugf("- fetchCopyReadyMessage request. Topic: %s, location: %s", topic, location);
        KafkaConsumer<String, byte[]> seeker;
        do {
            seeker = seekers.poll();
        } while (seeker == null);

        seeker.seek(new TopicPartition(topic, location.partition()), location.offset());

        try {
            return List.ofAll(seeker.poll(Duration.ofMillis(pollMaxDuration)))
                .map(ConsumerRecord::value)
                .headOption()
                .getOrElse(new byte[0]);
        } catch (Exception e) {
            Log.errorf("Exception while polling: %s", e);
            e.printStackTrace();
            return new byte[0];
        }
    }

    private KafkaConsumer<String, byte[]> getNewSeeker() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaBrokers);
        props.setProperty("group.id", "seeker-" + UUID.randomUUID());
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("max.poll.records", "1");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        return new KafkaConsumer<>(props);
    }

    private List<TopicPartition> getAllPartitions(Consumer<Object, Object> consumer, String topic) {
        Log.debugf("getAllPartitions - consumer: %s, topic: %s", consumer, topic);
        return List.ofAll(consumer.listTopics().values())
            .flatMap(partitionInfoList -> partitionInfoList)
            .filter(info -> info.topic().equals(topic))
            .map(info -> new TopicPartition(info.topic(), info.partition()));
    }

    @Incoming("segment-ready-router")
    @Outgoing("segment-ready-topic")
    public Flux<byte[]> trackerProcessor(Publisher<ReadyMeta> kafkaLocations) {
        return Flux.from(kafkaLocations)
            .flatMap(loc -> Mono.fromCallable(() -> fetchCopyReadyMessage(segmentReadyTopic, loc))
                .subscribeOn(Schedulers.boundedElastic())
            )
            .filter(bytes -> !(bytes.length == 0));
    }
}
