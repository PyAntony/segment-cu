package com.charter.pauselive.scu.kafka;

import com.github.javafaker.Faker;
import io.quarkus.arc.Unremovable;
import io.quarkus.logging.Log;
import io.vavr.collection.HashSet;
import io.vertx.core.Vertx;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import javax.enterprise.context.Dependent;
import javax.enterprise.inject.spi.CDI;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import com.charter.pauselive.scu.model.*;

/**
 * Seeker is a regular Kafka consumer with the task to fetch a message from a single partition/offset.
 */
@Dependent
@Unremovable
public class Seeker extends KafkaConsumer<String, byte[]> {
    int pollTries;
    int pollMaxDuration;
    final String identifier;

    static final private Faker faker = new Faker();
    static final public ConsumerRecord<String, byte[]> emptyRecord =
        new ConsumerRecord<>("???", 0, 0, "???", new byte[0]);

    public Seeker(
        @ConfigProperty(name = "%prod.kafka.bootstrap.servers") String kafkaBrokers,
        @ConfigProperty(name = "seeker.poll.tries", defaultValue = "3") int pollTriesNum,
        @ConfigProperty(name = "seeker.poll.duration.milli", defaultValue = "500") int pollDuration
    ) {
        super(getProperties(kafkaBrokers));

        var random = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        identifier = String.format("%s_%s", faker.name().firstName(), random);
        pollMaxDuration = pollDuration;
        pollTries = pollTriesNum;
    }

    public Seeker withAssignment(Collection<TopicPartition> topicPartitions) {
        assign(topicPartitions);
        return this;
    }

    public ConsumerRecord<String, byte[]> fetchRecord(String topic, int partition, long offset) {
        AtomicInteger emptyTries = new AtomicInteger(0);
        AtomicInteger exceptionTries = new AtomicInteger(0);

        return Mono.just(new TopicPartition(topic, partition))
            .doOnNext(topicPartition -> {
                activateSinglePartition(topicPartition);
                seek(topicPartition, offset);
            })
            .flatMap(__ -> Mono.fromCallable(() -> poll(Duration.ofMillis(pollMaxDuration))))
            .flatMap(records -> Flux.fromIterable(records)
                .filter(record -> correctRecord(record, partition, offset))
                .next()
                .switchIfEmpty(Mono.error(new RecordNotFoundException()))
            )
            .retryWhen(Retry.max(pollTries)
                .filter(e -> e instanceof RecordNotFoundException)
                .doAfterRetry(signal -> {
                    emptyTries.getAndIncrement();
                    Log.debugf("%s - empty records. Polling again...", identifier);
                })
            )
            .retryWhen(Retry.fixedDelay(2, Duration.ofMillis(500))
                .filter(e -> e instanceof KafkaException)
                .doAfterRetry(signal -> {
                    exceptionTries.getAndIncrement();
                    Log.warnf("%s - Pooling exception! %s", identifier, signal);
                })
            )
            .onErrorReturn(emptyRecord)
            .timed()
            .map(timed -> {
                var millis = timed.elapsedSinceSubscription().toMillis();
                var metrics = SeekerFetchMetrics.of(millis, emptyTries.get(), exceptionTries.get());
                timed.get().headers().add(SeekerFetchMetrics.key(), metrics.toString().getBytes());

                return timed.get();
            })
            .block();
    }

    public void terminateAsync() {
        Vertx.vertx().executeBlocking(future -> terminate());
    }

    private void terminate() {
        close();
        CDI.current().destroy(this);
        Log.debugf("Seeker %s was terminated...", identifier);
    }

    private boolean correctRecord(ConsumerRecord<?, ?> record, int partition, long offset) {
        return record.partition() == partition && record.offset() == offset;
    }

    private void activateSinglePartition(TopicPartition topicPartition) {
        if (!assignment().contains(topicPartition))
            assign(HashSet.ofAll(assignment()).add(topicPartition).toJavaSet());

        pause(assignment());
        resume(HashSet.of(topicPartition).toJavaSet());
    }

    private static Properties getProperties(String kafkaBrokers) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaBrokers);
        props.setProperty("group.id", "seeker-" + UUID.randomUUID());
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.offset.reset", "latest");
        props.setProperty("max.poll.records", "10");
        props.setProperty("max.partition.fetch.bytes", "1");
        props.setProperty("fetch.max.bytes", "1");
        props.setProperty("reconnect.backoff.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        return props;
    }

    public static Seeker getNew() {
        return CDI.current().select(Seeker.class).get();
    }

    public static class RecordNotFoundException extends Exception {
        public RecordNotFoundException() {
            super();
        }
    }
}
