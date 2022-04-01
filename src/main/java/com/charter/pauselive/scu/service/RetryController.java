package com.charter.pauselive.scu.service;

import com.charter.pauselive.scu.kafka.SegmentReadyRouter;
import com.charter.pauselive.scu.model.*;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;
import java.util.concurrent.LinkedBlockingQueue;

import static io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP;


@ApplicationScoped
public class RetryController {
    @ConfigProperty(name = "retry.controller.queue.max")
    int maxQueueCapacity;
    @ConfigProperty(name = "retry.controller.try.num")
    int maxCopyRetries;

    private LinkedBlockingQueue<SCUTracker> retryQueue;

    @Inject
    ReadyKeyCache readyKeyCache;
    @Inject
    SegmentReadyRouter segmentReadyRouter;
    @Inject
    Event<SCUTracker> retryDroppedEvent;

    @Inject
    void setQueue() {
        retryQueue = new LinkedBlockingQueue<>(maxQueueCapacity);
    }

    public boolean insert(IncomingKafkaRecord<String, PlayerCopyReady> message) {
        return retryQueue.offer(new SCUTracker(message.getPayload(), readyKeyCache));
    }

    public void emptyQueue() {
        retryQueue.clear();
    }

    @Scheduled(
        every = "{retry.controller.try.every.sec}",
        concurrentExecution = SKIP
    )
    void sendKafkaSeekLocations() {
        Log.debugf("scheduled retry - retryTrackerQueue size: %s", retryQueue.size());
        retryQueue.removeIf(tracker -> {
            if (tracker.readyKeysToRequest.isEmpty() || tracker.retries.get() >= maxCopyRetries) {
                retryDroppedEvent.fire(tracker);
                return true;
            } else {
                var locations = tracker.getSeekLocations(readyKeyCache);
                locations.forEach(loc -> segmentReadyRouter.sendSeekRequest(loc));
                tracker.retries.incrementAndGet();
                return false;
            }
        });
    }
}
