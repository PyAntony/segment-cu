package com.charter.pauselive.scu.service;

import com.charter.pauselive.scu.model.Payloads.*;
import com.charter.pauselive.scu.model.PlayerCopyReady;
import com.charter.pauselive.scu.model.ReadyKey;
import com.charter.pauselive.scu.model.SegmentReadyKey;
import io.quarkus.logging.Log;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.HashSet;
import io.vavr.collection.List;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vavr.API.For;

/**
 * Class maps PlayerCopyReady to SegmentReadyKey payloads.
 */
public class RequestsTracker {
    /**
     * Initial PlayerCopyReady message.
     */
    ABCPlayerCopyReady copyReadyRef;
    /**
     * On construction a PlayerCopyReady message is expanded to this list.
     * Each ReadyKey type is a key in ReadyKeyCache.
     */
    HashSet<ABCReadyKey> readyKeysToRequest;
    /**
     * Profiles sent to be seeked/fetched.
     */
    Set<ABCSegmentReadyKey> profilesSent = ConcurrentHashMap.newKeySet();
    /**
     * Number of times each key in `readyKeysToRequest` was searched for in cache.
     */
    public AtomicInteger retries = new AtomicInteger(0);

    public RequestsTracker(PlayerCopyReady copyReady, ReadyKeyCache cache, int maxRange) {
        long lastSegment = copyReady.lastProcessedSegment();
        long lastSegmentReq = lastSegment > 0 ? lastSegment : findLastSegment(copyReady, cache);
        long initSegment = copyReady.oldestSegment();
        long initSegmentReq = lastSegmentReq - initSegment > maxRange ? lastSegmentReq - maxRange : initSegment;
        Log.debugf(
            "ReqTracker(%s) - lastSegment, req: (%s, %s), initSegment, req: (%s, %s)",
            copyReady, lastSegment, lastSegmentReq, initSegment, initSegmentReq
        );

        readyKeysToRequest = initSegmentReq > 0 && lastSegmentReq > initSegmentReq ?
            HashSet.range(initSegmentReq, lastSegmentReq + 1)
                .map(segment -> ReadyKey.of(copyReady.src(), segment)) :
            HashSet.empty();

        copyReadyRef = copyReady;
    }

    public List<SegmentReadyKey> getNewSeekLocations(ReadyKeyCache cache) {
        var newSegmentReadyKeys = For(readyKeysToRequest, key ->
            For(cache.getReadyLocations(key)
                .map(meta -> (SegmentReadyKey) key.asSegmentReadyKey(meta))
                .map(readyKey -> readyKey.withCopyReadyRequestId(getCopyReadyRequestId()))
                .filter(segmentReadyKey -> !profilesSent.contains(segmentReadyKey))
            ).yield()
        );

        return List.ofAll(newSegmentReadyKeys)
            .asJava(list -> list.forEach(key -> {
                Log.debugf("Sending request to SegmentReadyRouter: %s", key);
                profilesSent.add(key);
            }));
    }

    public Tuple2<HashSet<ABCReadyKey>, HashSet<ABCSegmentReadyKey>> getSets() {
        return Tuple.of(readyKeysToRequest, HashSet.ofAll(profilesSent));
    }

    /**
     * Generate unique Id for the CopyReady payload.
     */
    public int getCopyReadyRequestId() {
        return copyReadyRef.hashCode();
    }

    /**
     * Find the greatest segment number in ReadyKeyCache cache.
     *
     * @param copyReady copyReady payload.
     * @param cache     ReadyKeyCache cache.
     * @return greatest segment number or -1 if source is not found.
     */
    private long findLastSegment(PlayerCopyReady copyReady, ReadyKeyCache cache) {
        return cache.readyKeysOlderThan(copyReady.oldestSegment(), copyReady.src())
            .map(ABCReadyKey::segmentNumber)
            .max()
            .getOrElse(-1L);
    }
}
