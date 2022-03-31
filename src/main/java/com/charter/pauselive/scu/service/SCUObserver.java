package com.charter.pauselive.scu.service;

import com.charter.pauselive.scu.model.PlayerCopyReady;
import com.charter.pauselive.scu.model.SegmentReady;
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

    // LinkedBlockingQueue<PlayerCopyReady> copyReadyQueue;
    // LinkedBlockingQueue<ByteBuffer> segmentReadyQueue;
    // ConcurrentHashMap<String, RetryTracker> retryTrackerHistory;

    // @Inject
    // void setStructures() {
    //     copyReadyQueue = new LinkedBlockingQueue<>(trackHistory ? Integer.MAX_VALUE : 0);
    //     segmentReadyQueue = new LinkedBlockingQueue<>(trackHistory ? Integer.MAX_VALUE : 0);
    //     retryTrackerHistory = trackHistory ? new ConcurrentHashMap<>() : null;
    // }

    // void onCopyReadyEnqueued(@Observes PlayerCopyReady copyReady) {
    //     copyReadyQueue.offer(copyReady);
    // }

    // void onRetryTrackerDropped(@Observes RetryTracker retryTracker) {
    //     if (retryTrackerHistory != null)
    //         retryTrackerHistory.putIfAbsent(retryTracker.copyReadyReqId, retryTracker);
    // }

    // void onSegmentReadySent(@Observes ByteBuffer segmentReadySerialized) {
    //     segmentReadyQueue.offer(segmentReadySerialized);
    // }
}
