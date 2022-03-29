package com.charter.pauselive.scu.service;

import io.quarkus.logging.Log;
import io.vavr.Function1;
import io.vavr.collection.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Helpers {

    /***
     * Calculate the initial time of the bucket where {@code currTimestamp} sits with respect to
     * a timestamp {@code anchor}. Function expects timestamps in seconds (10 digits). Example:
     * <br><br>
     * anchor = 2022-11-13T00:00:00Z<br>
     * currTimestamp = 2022-11-13T00:00:13Z<br>
     * windowSec = 5<br>
     *
     * result = 2022-11-13T00:00:10Z
     *
     * @param anchor starting timestamp.
     * @param windowSec length of buckets in seconds
     * @param currTimestamp the referred timestamp.
     * @param fmt a String formatter to map the timestamp.
     */
    public static String startTimeFromAnchor(
        long anchor, int windowSec, Function1<Long, String> fmt, long currTimestamp
    ) {
        return fmt.apply(anchor + ((currTimestamp - anchor) / windowSec) * windowSec);
    }

    public static void seekOffsetsAtTimestamp(
        Consumer<?, ?> consumer, Collection<TopicPartition> partitions, long shouldStartAt
    ) {
        String startTime = Instant.ofEpochMilli(shouldStartAt).toString();
        Log.debugf("onPartitionsAssigned - consumption starts at %s", startTime);

        Map<TopicPartition, Long> request = new HashMap<>();
        List.ofAll(partitions).map(partition -> request.put(partition, shouldStartAt));

        List.ofAll(consumer.offsetsForTimes(request).entrySet())
            .forEach(entry -> {
                Log.debugf("seekOffsets - sought: (%s -> %s)", entry.getKey(), entry.getValue());
                consumer.seek(
                    entry.getKey(),
                    entry.getValue() == null ? 0L : entry.getValue().offset()
                );
            });
    }
}
