package com.charter.pauselive.scu.model;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Auxiliary;

public class Payloads {
    @Immutable
    public static abstract class ABCSegmentReadyKey {
        public abstract String source();
        public abstract String profile();
        public abstract long segmentNumber();
        public abstract int partition();
        public abstract long offset();

        public ABCReadyKey getKey() {
            return ReadyKey.of(source(), segmentNumber());
        }

        public ABCReadyMeta getMetadata(long ingestion) {
            return ReadyMeta.of(profile(), partition(), offset(), ingestion);
        }
    }

    @Immutable
    public static abstract class ABCReadyKey {
        public abstract String source();
        public abstract long segmentNumber();
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
}
