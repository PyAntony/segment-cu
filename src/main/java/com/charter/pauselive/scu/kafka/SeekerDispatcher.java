package com.charter.pauselive.scu.kafka;

import io.quarkus.arc.Lock;
import io.quarkus.logging.Log;
import io.vavr.collection.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 2 observations:
 * - It's better to let each Consumer handle a single partition.
 * - Best combination of consumer parameters must be tested.
 */
@ApplicationScoped
public class SeekerDispatcher {
    int seekersLimit;
    String segmentReadyTopic;

    private final LinkedBlockingQueue<Seeker> seekers;
    private List<TopicPartition> segmentReadyPartitions;

    public SeekerDispatcher(
        @ConfigProperty(name = "dispatcher.seekers.limit") int seekersLimit,
        @ConfigProperty(name = "dispatcher.segmentready.topic") String segmentReadyTopic
    ) {
        this.seekersLimit = seekersLimit;
        this.segmentReadyTopic = segmentReadyTopic;
        this.segmentReadyPartitions = List.empty();

        seekers = new LinkedBlockingQueue<>(seekersLimit);
        List.range(0, seekersLimit).forEach(__ -> seekers.offer(Seeker.getNew()));
    }

    @PostConstruct
    void assignSeekers() {
        assignSeekers(segmentReadyTopic);
    }

    public ConsumerRecord<String, byte[]> search(int partition, long offset) {
        Seeker seeker;
        do {
            seeker = seekers.poll();
        } while (seeker == null);

        var record = seeker.fetchRecord(segmentReadyTopic, partition, offset);

        if (record.value().length > 0)
            seekers.offer(seeker);
        else {
            seeker.terminateAsync();
            seekers.offer(Seeker.getNew().withAssignment(segmentReadyPartitions.toJavaList()));
        }

        return record;
    }

    /**
     * Assign all seekers to all partitions in `topic`.
     */
    @Lock
    void assignSeekers(String topic) {
        Seeker seeker = Seeker.getNew();
        seeker.subscribe(List.of(topic).toJavaList());
        segmentReadyPartitions = getAllPartitions(seeker, topic);
        Log.infof("TopicPartitions found for seekers: %s", segmentReadyPartitions);

        seeker.terminateAsync();
        seekers.forEach(consumer -> consumer.assign(segmentReadyPartitions.toJavaList()));

        if (segmentReadyPartitions.isEmpty())
            Log.fatal("No partitions found for segment-ready topic!");
    }

    /**
     * Produces list of topicPartitions for a single `topic`.
     */
    private List<TopicPartition> getAllPartitions(Seeker consumer, String topic) {
        Log.infof("subscribedSeeker - consumer assignments: %s, topic: %s", consumer.assignment(), topic);
        Log.infof("subscribedSeeker - topics found: %s", consumer.listTopics().keySet());
        return List.ofAll(consumer.listTopics().values())
            .flatMap(partitionInfoList -> partitionInfoList)
            .filter(info -> info.topic().equals(topic))
            .map(info -> new TopicPartition(info.topic(), info.partition()));
    }
}
