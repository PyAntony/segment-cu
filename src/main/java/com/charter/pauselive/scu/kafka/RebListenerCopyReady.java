package com.charter.pauselive.scu.kafka;

import com.charter.pauselive.scu.service.Helpers;
import com.charter.pauselive.scu.service.RetryController;
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
@Identifier("ready-copy-rebalancer")
public class RebListenerCopyReady implements KafkaConsumerRebalanceListener {
    @ConfigProperty(name = "readycopy.seek.before.sec")
    int seekBeforeSeconds;

    @Inject
    RetryController retryController;

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        retryController.emptyQueue();

        long shouldStartAt = Instant.now().minus(seekBeforeSeconds, ChronoUnit.SECONDS).toEpochMilli();
        Helpers.seekOffsetsAtTimestamp(consumer, partitions, shouldStartAt);
    }
}
