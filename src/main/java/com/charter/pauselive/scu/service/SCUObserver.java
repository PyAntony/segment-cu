package com.charter.pauselive.scu.service;

import com.charter.pauselive.scu.model.Payloads.*;
import com.charter.pauselive.scu.annot.EventTypes.SeekSuccess;
import io.quarkus.logging.Log;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class SCUObserver {
    @ConfigProperty(name = "observer.track.history")
    boolean trackHistory;

    ConcurrentHashMap<SCUTracker, Integer> retryTrackerHistory;

    @Inject
    void setContainers() {
        retryTrackerHistory = new ConcurrentHashMap<>();
    }

    void onRetryTrackerDropped(@Observes SCUTracker scuTracker) {
        if (trackHistory)
            retryTrackerHistory.putIfAbsent(scuTracker, scuTracker.profilesSent.size());
    }

    void onSegmentReadySent(@Observes @SeekSuccess(value = true) ABCSegmentReadyKey segmentReadyKey) {
        Log.tracef("fetchSegmentReady succeeded: %s", segmentReadyKey);
    }

    void onSegmentReadyFailure(@Observes @SeekSuccess(value = false) ABCSegmentReadyKey segmentReadyKey) {
        Log.warnf(
            "fetchSegmentReady failed after retries: %s. Fallback sent: %s",
            segmentReadyKey,
            segmentReadyKey.fallbackMessage()
        );
    }
}
