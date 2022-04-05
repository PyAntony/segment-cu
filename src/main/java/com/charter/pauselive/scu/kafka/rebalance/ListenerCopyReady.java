package com.charter.pauselive.scu.kafka.rebalance;

import com.charter.pauselive.scu.service.Helpers;
import com.charter.pauselive.scu.service.SegmentKeyFinder;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;

@ApplicationScoped
@Identifier("ready-copy-listener")
public class ListenerCopyReady implements KafkaConsumerRebalanceListener {
    @ConfigProperty(name = "readycopy.seek.before.sec")
    int seekBeforeSeconds;

    @Inject
    SegmentKeyFinder segmentKeyFinder;

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        segmentKeyFinder.emptyQueue();

        long shouldStartAt = Instant.now().minus(seekBeforeSeconds, ChronoUnit.SECONDS).toEpochMilli();
        Helpers.seekOffsetsAtTimestamp(consumer, partitions, shouldStartAt);
    }
}
