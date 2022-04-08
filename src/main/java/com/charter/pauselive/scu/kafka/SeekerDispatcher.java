package com.charter.pauselive.scu.kafka;

import io.quarkus.arc.Lock;
import io.quarkus.logging.Log;
import io.vavr.collection.List;
import io.vavr.control.Try;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

@ApplicationScoped
public class SeekerDispatcher {
    @ConfigProperty(name = "%prod.kafka.bootstrap.servers")
    String kafkaBrokers;
    @ConfigProperty(name = "dispatcher.consumers.num")
    int consumersNum;
    @ConfigProperty(name = "dispatcher.poll.duration.milli")
    int pollMaxDuration;

    static final ConsumerRecord<String, byte[]> emptyRecord =
        new ConsumerRecord<>("???", 0, 0, "???", new byte[0]);

    private LinkedBlockingQueue<KafkaConsumer<String, byte[]>> seekers;
    private List<TopicPartition> assignedTopicPartitions = List.empty();

    @Inject
    void initializeSeekers() {
        seekers = new LinkedBlockingQueue<>(consumersNum);
        List.range(0, consumersNum).forEach(__ -> seekers.offer(getNewSeeker()));
    }

    public ConsumerRecord<String, byte[]> getEmptyRecord() {
        return emptyRecord;
    }

    public int getAvailableSeekers() {
        return consumersNum;
    }

    public ConsumerRecord<String, byte[]> search(String topic, int partition, long offset) {
        KafkaConsumer<String, byte[]> seeker;
        do {
            seeker = seekers.poll();
        } while (seeker == null);

        seeker.seek(new TopicPartition(topic, partition), offset);
        var record = fetchRecord(seeker, pollMaxDuration);
        seekers.offer(seeker);

        return record;
    }

    private ConsumerRecord<String, byte[]> fetchRecord(KafkaConsumer<String, byte[]> seeker, long pollDuration) {
        return Try.of(() -> seeker.poll(Duration.ofMillis(pollDuration)))
            .map(records -> List.ofAll(records).headOption())
            .map(option -> option.getOrElse(emptyRecord))
            .onFailure(e -> Log.errorf("Exception while polling: %s", e))
            .getOrElse(emptyRecord);
    }

    private List<TopicPartition> getAllPartitions(KafkaConsumer<String, byte[]> consumer, String topic) {
        Log.infof("subscribedSeeker - consumer assignments: %s, topic: %s", consumer.assignment(), topic);
        Log.infof("subscribedSeeker - topics found: %s", consumer.listTopics().keySet());
        return List.ofAll(consumer.listTopics().values())
            .flatMap(partitionInfoList -> partitionInfoList)
            .filter(info -> info.topic().equals(topic))
            .map(info -> new TopicPartition(info.topic(), info.partition()));
    }

    @Lock
    void assignSeekers(String topic) {
        if (assignedTopicPartitions.isEmpty()) {
            var anyConsumer = seekers.poll();
            anyConsumer.subscribe(List.of(topic).toJavaList());

            assignedTopicPartitions = getAllPartitions(anyConsumer, topic);
            Log.infof("TopicPartitions found for seekers: %s", assignedTopicPartitions);

            anyConsumer.unsubscribe();
            seekers.offer(anyConsumer);
            seekers.forEach(consumer -> consumer.assign(assignedTopicPartitions.toJavaList()));
        }

        if (assignedTopicPartitions.isEmpty())
            Log.error("No partitions found for segment-ready topic!");
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
}
