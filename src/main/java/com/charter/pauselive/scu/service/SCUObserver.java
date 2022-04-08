package com.charter.pauselive.scu.service;

import com.charter.pauselive.scu.annot.EventTypes.SeekSuccess;
import io.quarkus.logging.Log;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class SCUObserver {
    @ConfigProperty(name = "observer.debug.mode")
    boolean debugMode;

    ConcurrentHashMap<RequestsTracker, Integer> retryTrackerHistory;

    @Inject
    void setContainers() {
        retryTrackerHistory = new ConcurrentHashMap<>();
    }

    void onRetryTrackerDropped(@Observes RequestsTracker scuTracker) {
//        if (debugMode)
//            retryTrackerHistory.putIfAbsent(scuTracker, scuTracker.profilesSent.size());
    }

    void onSeekSuccess(@Observes @SeekSuccess(value = true) SeekEvent seekEvent) {
        Log.debugf(
            "Seek event succeeded: %s",
            debugMode ? seekEvent.getDetailedRepr() : seekEvent.getRepr()
        );
    }

    void onSeekFailure(@Observes @SeekSuccess(value = false) SeekEvent seekEvent) {

    }
}
