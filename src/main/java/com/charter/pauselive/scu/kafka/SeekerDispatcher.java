package com.charter.pauselive.scu.kafka;

import io.quarkus.arc.Lock;
import io.quarkus.logging.Log;
import io.vavr.collection.List;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

@ApplicationScoped
public class SeekerDispatcher {
    @ConfigProperty(name = "%prod.kafka.bootstrap.servers")
    String kafkaBrokers;
    @ConfigProperty(name = "readyrouter.consumers.num")
    int consumersNum;

    private LinkedBlockingQueue<KafkaConsumer<String, byte[]>> seekers;
    private List<TopicPartition> assignedTopicPartitions = List.empty();

    @Inject
    void initializeSeekers() {
        seekers = new LinkedBlockingQueue<>(consumersNum);
        List.range(0, consumersNum).forEach(__ -> seekers.offer(getNewSeeker()));
    }

    public KafkaConsumer<String, byte[]> getSeeker() {
        KafkaConsumer<String, byte[]> seeker;
        do {
            seeker = seekers.poll();
        } while (seeker == null);

        return seeker;
    }

    public void regainSeeker(KafkaConsumer<String, byte[]> consumer) {
        seekers.offer(consumer);
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
            var anyConsumer = getSeeker();
            anyConsumer.subscribe(List.of(topic).toJavaList());

            assignedTopicPartitions = getAllPartitions(anyConsumer, topic);
            Log.infof("TopicPartitions found for seekers: %s", assignedTopicPartitions);

            anyConsumer.unsubscribe();
            regainSeeker(anyConsumer);
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
