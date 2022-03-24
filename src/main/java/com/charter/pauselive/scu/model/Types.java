package com.charter.pauselive.scu.model;

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
}
