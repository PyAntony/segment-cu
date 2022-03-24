package com.charter.pauselive.scu.kafka;

import io.quarkus.logging.Log;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.vavr.collection.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
@Identifier("ready-key-rebalancer")
public class RebalanceListenerReadyKey implements KafkaConsumerRebalanceListener {
    @ConfigProperty(name = "readykey.cache.ttl.seconds")
    private int ttlSeconds;

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        long shouldStartAt = Instant.now().minus(ttlSeconds, ChronoUnit.SECONDS).toEpochMilli();
        String startTime = Instant.ofEpochMilli(shouldStartAt).toString();
        Log.debugf("onPartitionsAssigned - 'segment-ready-keys' consumption starts at %s", startTime);

        Map<TopicPartition, Long> request = new HashMap<>();
        List.ofAll(partitions).map(partition -> request.put(partition, shouldStartAt));

        List.ofAll(consumer.offsetsForTimes(request).entrySet())
            .forEach(entry -> {
                Log.debugf("onPartitionsAssigned - assignments: (%s, %s)", entry.getKey(), entry.getValue());
                consumer.seek(
                    entry.getKey(),
                    entry.getValue() == null ? 0L : entry.getValue().offset()
                );
            });
    }

}
