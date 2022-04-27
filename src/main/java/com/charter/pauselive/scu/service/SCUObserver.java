package com.charter.pauselive.scu.service;

import com.charter.pauselive.scu.annot.EventTypes.SeekSuccess;
import io.quarkus.logging.Log;
import io.vavr.collection.List;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.charter.pauselive.scu.model.Payloads.*;

/**
 * Observes events fired by other application classes. It also stores this events
 * for later analysis.
 */
@ApplicationScoped
public class SCUObserver {
    @ConfigProperty(name = "observer.debug.mode")
    boolean debugMode;
    @ConfigProperty(name = "observer.history.track")
    boolean trackHistory;

    ConcurrentHashMap<Integer, RequestsTracker> requestTrackerHistory;
    ConcurrentHashMap<Integer, List<SeekEvent>> successfulHistory;

    @Inject
    void setContainers() {
        requestTrackerHistory = new ConcurrentHashMap<>();
        successfulHistory = new ConcurrentHashMap<>();
    }

    void onRetryTrackerDropped(@Observes RequestsTracker scuTracker) {
        if (trackHistory) {
            requestTrackerHistory.putIfAbsent(scuTracker.getCopyReadyRequestId(), scuTracker);
        }
    }

    void onSeekSuccess(@Observes @SeekSuccess(value = true) SeekEvent seekEvent) {
        Log.infof("Seek event succeeded: %s", seekEvent.asInfoPayload(debugMode));

        if (trackHistory) {
            successfulHistory.compute(
                seekEvent.getReadyKey().copyReadyRequestId(),
                (k, list) -> list == null ? List.of(seekEvent) : list.append(seekEvent)
            );
        }
    }

    void onSeekFailure(@Observes @SeekSuccess(value = false) SeekEvent seekEvent) {
        String base = seekEvent.getRecord().value().length == 0 ?
            "SegmentReady not found for %s" :
            "Segment Ready found, but producer failed to send message for %s";

        Log.warnf(base, seekEvent.getReadyKey());
    }

    public ConcurrentHashMap<Integer, RequestsTracker> getRequestTrackerHistory() {
        return requestTrackerHistory;
    }

    public ConcurrentHashMap<Integer, List<SeekEvent>> getSuccessfulHistory() {
        return successfulHistory;
    }
}
