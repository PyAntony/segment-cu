package com.charter.pauselive.scu.service;

import javax.enterprise.context.ApplicationScoped;

import com.charter.pauselive.scu.model.*;
import com.charter.pauselive.scu.model.Payloads.*;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vavr.collection.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import io.quarkus.arc.Lock;

@ApplicationScoped
public class ReadyKeyCache {
    @ConfigProperty(name = "readykey.time.anchor")
    private int keyTimeAnchor;
    @ConfigProperty(name = "readykey.key.bucket.sec")
    private int aggBucketSec;
    @ConfigProperty(name = "readykey.cache.ttl.seconds")
    private int ttlMaxSeconds;

    ConcurrentHashMap<ABCReadyKey, HashSet<ABCReadyMeta>> sourceSegmentMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<KeyTimeWindow, List<ABCReadyKey>> timeTracker = new ConcurrentHashMap<>();

    @Lock(value = Lock.Type.READ)
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

        timeTracker.compute(keyTime,
            (key, list) -> list != null ?
                list.append(payload.getKey()) :
                List.of(payload.getKey())
        );
    }

    public List<ABCReadyKey> readyKeysOlderThan(long segmentNumber) {
        return List.ofAll(sourceSegmentMap.keySet())
            .filter(key -> key.segmentNumber() > segmentNumber);
    }

    public HashSet<ABCReadyMeta> getReadyLocations(ABCReadyKey key) {
        var payloads = sourceSegmentMap.get(key);
        return payloads == null ? HashSet.empty() : payloads;
    }

    @Lock
    @Scheduled(every = "{readykey.cache.prune.every.sec}")
    void pruneExpiredKeys() {
        var prunedTracker = new ConcurrentHashMap<KeyTimeWindow, List<ABCReadyKey>>();
        timeTracker.forEach((timeKey, readyKeys) -> {
            if (timeKey.secondsFromNow() > ttlMaxSeconds) {
                Log.debugf("pruneExpiredKeys - Removing entries for timeKey: %s", timeKey);
                readyKeys.forEach(key -> sourceSegmentMap.remove(key));
            } else
                prunedTracker.put(timeKey, readyKeys);
        });

        this.timeTracker = prunedTracker;
    }

    private String produceTimeKey(long timestampMilli) {
        return Helpers.startTimeFromAnchor(
            keyTimeAnchor,
            aggBucketSec,
            l -> Instant.ofEpochSecond(l).toString(),
            timestampMilli / 1000
        );
    }
}