package com.charter.pauselive.scu.service;

import com.charter.pauselive.scu.model.Payloads.*;
import com.charter.pauselive.scu.model.PlayerCopyReady;
import com.charter.pauselive.scu.model.ReadyKey;
import io.quarkus.logging.Log;
import io.vavr.collection.List;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vavr.API.For;

public class SCUTracker {
    ABCPlayerCopyReady copyReadyRef;
    List<ABCReadyKey> readyKeysToRequest;
    ConcurrentHashMap<ABCSegmentReadyKey, Boolean> profilesSent = new ConcurrentHashMap<>();
    public AtomicInteger retries = new AtomicInteger(0);

    public SCUTracker(PlayerCopyReady copyReady, ReadyKeyCache cache) {
        long lastSegment = copyReady.lastProcessedSegment();
        long maxSegment = lastSegment > 0 ? lastSegment : estimateLastSegment(copyReady.oldestSegment(), cache);

        readyKeysToRequest = maxSegment > copyReady.oldestSegment() ?
            List.range(copyReady.oldestSegment(), maxSegment + 1)
                .map(segment -> ReadyKey.of(copyReady.src(), segment)) :
            List.empty();

        copyReadyRef = copyReady;
    }

    public List<ABCSegmentReadyKey> getSeekLocations(ReadyKeyCache cache) {
        var newSegmentReadyKeys = For(readyKeysToRequest, key ->
            For(cache.getReadyLocations(key)
                .map(key::asSegmentReadyKey)
                .filter(segmentReadyKey -> !profilesSent.containsKey(segmentReadyKey))
            ).yield()
        );

        return List.ofAll(newSegmentReadyKeys)
            .asJava(list -> list.forEach(key -> {
                Log.debugf("Sending request to SegmentReadyRouter: %s", key);
                profilesSent.put(key, false);
            }));
    }

    public void requestSucceeded(ABCSegmentReadyKey segmentReadyKey) {
        profilesSent.computeIfPresent(segmentReadyKey, (k, v) -> true);
    }

    private long estimateLastSegment(long oldestSegment, ReadyKeyCache cache) {
        return cache.readyKeysOlderThan(oldestSegment)
            .map(ABCReadyKey::segmentNumber)
            .max()
            .getOrElse(-1L);
    }
}
