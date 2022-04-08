package com.charter.pauselive.scu.service;

import com.charter.pauselive.scu.model.Payloads.*;
import com.charter.pauselive.scu.model.PlayerCopyReady;
import com.charter.pauselive.scu.model.ReadyKey;
import io.quarkus.logging.Log;
import io.vavr.collection.HashSet;
import io.vavr.collection.List;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vavr.API.For;

/**
 * Class with logic to map PlayerCopyReady to SegmentReadyKey payloads.
 */
public class RequestsTracker {
    /**
     * Initial PlayerCopyReady message.
     */
    ABCPlayerCopyReady copyReadyRef;
    /**
     * On construction a PlayerCopyReady message is expanded to this list.
     * Each ReadyKey type represents a mapping in ReadyKeyCache.
     */
    HashSet<ABCReadyKey> readyKeysToRequest;
    /**
     * Mapping of profiles sent to be seeked/fetched. Boolean values can be used to mark success of final
     * rerouting (message was fetched and resent successfully).
     */
    ConcurrentHashMap<ABCSegmentReadyKey, Boolean> profilesSent = new ConcurrentHashMap<>();
    /**
     * Number of times each key in `readyKeysToRequest` was searched for in cache.
     */
    public AtomicInteger retries = new AtomicInteger(0);

    public RequestsTracker(PlayerCopyReady copyReady, ReadyKeyCache cache) {
        long lastSegment = copyReady.lastProcessedSegment();
        long maxSegment = lastSegment > 0 ? lastSegment : estimateLastSegment(copyReady.oldestSegment(), cache);

        readyKeysToRequest = maxSegment > copyReady.oldestSegment() ?
            HashSet.range(copyReady.oldestSegment(), maxSegment + 1)
                .map(segment -> ReadyKey.of(copyReady.src(), segment)) :
            HashSet.empty();

        copyReadyRef = copyReady;
    }

    public List<ABCSegmentReadyKey> getNewSeekLocations(ReadyKeyCache cache) {
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
