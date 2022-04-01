package com.charter.pauselive.scu.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Parameter;

public class Payloads {
    @Immutable
    public static abstract class ABCSegmentReadyKey {
        public abstract String source();
        public abstract String profile();
        public abstract long segmentNumber();
        public abstract int partition();
        public abstract long offset();

        @JsonIgnore
        public ABCReadyKey getKeyPair() {
            return ReadyKey.of(source(), segmentNumber());
        }

        @JsonIgnore
        public ABCReadyMeta getMetadata(long ingestion) {
            return ReadyMeta.of(profile(), partition(), offset(), ingestion);
        }
    }

    /**
     * Used for ReadyKeyCache and RetryTracker payloadsSent cache.
     */
    @Immutable
    public static abstract class ABCReadyKey {
        public abstract String source();
        public abstract long segmentNumber();

        @Default
        @Parameter(false)
        public String profile() {
            return "UNDEFINED";
        }

        public ABCReadyKey withDefinedProfile(String profile) {
            return ReadyKey.builder()
                .source(source())
                .segmentNumber(segmentNumber())
                .profile(profile)
                .build();
        }

        public ABCSegmentReadyKey asSegmentReadyKey(ABCReadyMeta meta) {
            return SegmentReadyKey.of(source(), meta.profile(), segmentNumber(), meta.partition(), meta.offset());
        }
    }

    @Immutable
    public static abstract class ABCReadyMeta {
        public abstract String profile();
        @Auxiliary
        public abstract int partition();
        @Auxiliary
        public abstract long offset();
        @Auxiliary
        public abstract long ingestionTime();

        @Override
        public String toString() {
            return String.format(
                "ReadyMeta{profile=%s, partition=%s, offset=%s}",
                profile(), partition(), offset()
            );
        }
    }

    @Immutable
    public static abstract class ABCPlayerCopyReady {
        public abstract String src();
        public abstract long oldestSegment();
        // service must handle '-1' case
        public abstract long lastProcessedSegment();
    }

    @Immutable
    public static abstract class ABCSegmentReady {
        public abstract String source();
        public abstract String bucket();
        public abstract String version();
        public abstract String encodedSegment();
        public abstract String fileName();
    }
}
