package com.charter.pauselive.scu.cache;

import javax.enterprise.context.ApplicationScoped;

import com.charter.pauselive.scu.model.*;
import com.charter.pauselive.scu.model.Payloads.*;
import com.charter.pauselive.scu.utils.Helpers;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vavr.collection.*;
import lombok.Synchronized;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Instant;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class ReadyKeyCache {
    @ConfigProperty(name = "readykey.time.anchor")
    private int keyTimeAnchor;
    @ConfigProperty(name = "readykey.key.bucket.sec")
    private int aggBucketSec;
    @ConfigProperty(name = "readykey.cache.ttl.seconds")
    private int ttlMaxSeconds;

    static Comparator<KeyTimeWindow> keyTimeComparator = Comparator.comparing(KeyTimeWindow::value);

    TreeMap<KeyTimeWindow, List<ABCReadyKey>> timeTracker = TreeMap.empty(keyTimeComparator);
    ConcurrentHashMap<ABCReadyKey, HashSet<ABCReadyMeta>> sourceSegmentMap = new ConcurrentHashMap<>();

    public void insert(IncomingKafkaRecord<String, SegmentReadyKey> readyKeyMessage) {
        var payload = readyKeyMessage.getPayload();
        var timestamp = readyKeyMessage.getTimestamp().toEpochMilli();
        var newProfile = payload.getMetadata(timestamp);
        var keyTime = KeyTimeWindow.of(produceTimeKey(timestamp));

        sourceSegmentMap.compute(payload.getKey(),
            (key, set) -> set != null ?
                set.remove(newProfile).add(newProfile) :
                HashSet.of(newProfile)
        );

        timeTrackerInsertion(keyTime, payload.getKey());
    }

    @Synchronized
    private void timeTrackerInsertion(KeyTimeWindow keyTime, ABCReadyKey readyKey) {
        var readyKeys = timeTracker.getOrElse(keyTime, List.empty()).append(readyKey);
        timeTracker = timeTracker.put(keyTime, readyKeys);
    }

    @Synchronized
    private void pruneExpiredKeys() {
        var expired = timeTracker.takeWhile(tp -> tp._1.secondsFromNow() > ttlMaxSeconds);
        Log.debugf("timeTracker keys: %s", timeTracker.keySet());
        Log.debugf("Removing expired keys: %s", expired.keySet());

        expired.flatMap(tp -> {
            Log.debugf("Pruning cache - timeKey: %s, readyKeys to remove: %s", tp._1, tp._2.size());
            return tp._2;
        }).forEach(readyKey -> sourceSegmentMap.remove(readyKey));

        timeTracker = timeTracker.drop(expired.size());
    }

    private String produceTimeKey(long timestampMilli) {
        return Helpers.startTimeFromAnchor(
            keyTimeAnchor,
            aggBucketSec,
            l -> Instant.ofEpochSecond(l).toString(),
            timestampMilli / 1000
        );
    }

    @Scheduled(every = "{readykey.cache.prune.every.sec}")
    void runPrunerTask() {
        pruneExpiredKeys();
    }
}