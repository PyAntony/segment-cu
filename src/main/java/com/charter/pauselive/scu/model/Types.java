package com.charter.pauselive.scu.model;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.time.Instant;

public class Types {
    static abstract class Wrapper<T> {
        public abstract T value();

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + value() + ")";
        }
    }

    @Immutable(builder = false, copy = false)
    static abstract class ABCKeyTimeWindow extends Wrapper<String> {
        public int compareTo(ABCKeyTimeWindow that) {
            return this.value().compareTo(that.value());
        }

        public long secondsFromNow() {
            var instant1 = Instant.parse(this.value());
            var instant2 = Instant.now();

            return Duration.between(instant1, instant2).abs().toSeconds();
        }

        public long secondsBetween(ABCKeyTimeWindow that) {
            var instant1 = Instant.parse(this.value());
            var instant2 = Instant.parse(that.value());

            return Duration.between(instant1, instant2).abs().toSeconds();
        }
    }

    @Immutable(builder = false, copy = false)
    static abstract class ABCKafkaMetadata {
        public abstract RecordMetadata meta();

        @Override
        public String toString() {
            return String.format(
                "KafkaMetadata{topic=%s, partition=%s, offset=%s, timestamp=%s}",
                meta().topic(), meta().partition(), meta().offset(), meta().timestamp()
            );
        }
    }

    @Immutable(builder = false, copy = false)
    static abstract class ABCKafkaRecordMeta {
        public abstract ConsumerRecord<String, ?> record();

        @Override
        public String toString() {
            return String.format(
                "RecordMetadata{topic=%s, partition=%s, offset=%s, timestamp=%s, key=%s}",
                record().topic(), record().partition(), record().offset(), record().timestamp(), record().key()
            );
        }
    }
}
