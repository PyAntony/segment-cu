package com.charter.pauselive.scu.kafka;

import com.charter.pauselive.scu.service.Helpers;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;

@ApplicationScoped
@Identifier("ready-key-rebalancer")
public class RebListenerReadyKey implements KafkaConsumerRebalanceListener {
    @ConfigProperty(name = "readykey.cache.ttl.seconds")
    private int ttlSeconds;

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        long shouldStartAt = Instant.now().minus(ttlSeconds, ChronoUnit.SECONDS).toEpochMilli();
        Helpers.seekOffsetsAtTimestamp(consumer, partitions, shouldStartAt);
    }
}
