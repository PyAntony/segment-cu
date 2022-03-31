package com.charter.pauselive.scu.service;

import com.charter.pauselive.scu.model.PlayerCopyReady;
import com.charter.pauselive.scu.model.SegmentReady;
import io.quarkus.logging.Log;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

@ApplicationScoped
public class SCUObserver {
    @ConfigProperty(name = "observer.track.history")
    boolean trackHistory;

    LinkedBlockingQueue<PlayerCopyReady> copyReadyQueue;
    LinkedBlockingQueue<ByteBuffer> segmentReadyQueue;
    ConcurrentHashMap<String, RetryTracker> retryTrackerHistory;

    @Inject
    void setStructures() {
        Log.debugf("SCUObserver is tracking history: %s", trackHistory);
        copyReadyQueue = new LinkedBlockingQueue<>(trackHistory ? Integer.MAX_VALUE : 1);
        segmentReadyQueue = new LinkedBlockingQueue<>(trackHistory ? Integer.MAX_VALUE : 1);
        retryTrackerHistory = new ConcurrentHashMap<>();
    }

    void onCopyReadyEnqueued(@Observes PlayerCopyReady copyReady) {
        if (trackHistory)
            copyReadyQueue.offer(copyReady);
    }

    void onRetryTrackerDropped(@Observes RetryTracker retryTracker) {
        if (trackHistory)
            retryTrackerHistory.putIfAbsent(retryTracker.copyReadyReqId, retryTracker);
    }

    void onSegmentReadySent(@Observes ByteBuffer segmentReadySerialized) {
        if (trackHistory)
            segmentReadyQueue.offer(segmentReadySerialized);
    }
}
