package com.charter.pauselive.scu.service;

import com.charter.pauselive.scu.model.Payloads.*;
import com.charter.pauselive.scu.model.*;

import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import static io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP;


@ApplicationScoped
public class KeyFinderCache {
    @ConfigProperty(name = "keyfinder.readykey.queue.threshold")
    int maxReadyKeyQueueSizeAllowed;
    @ConfigProperty(name = "keyfinder.queues.max.size")
    int queuesSizeLimit;
    @ConfigProperty(name = "keyfinder.try.num")
    int maxCopyRetries;

    private LinkedBlockingQueue<RequestsTracker> retryQueue;
    private LinkedBlockingQueue<ABCSegmentReadyKey> readyKeyQueue;

    @Inject
    ReadyKeyCache readyKeyCache;
    @Inject
    Event<RequestsTracker> retryDroppedEvent;

    @Inject
    void setQueues() {
        retryQueue = new LinkedBlockingQueue<>(queuesSizeLimit);
        readyKeyQueue = new LinkedBlockingQueue<>(queuesSizeLimit);
    }

    public boolean insert(IncomingKafkaRecord<String, PlayerCopyReady> message) {
        if (readyKeyQueue.size() < maxReadyKeyQueueSizeAllowed) {
            retryQueue.offer(new RequestsTracker(message.getPayload(), readyKeyCache));
            return true;
        }

        return false;
    }

    public ArrayList<ABCSegmentReadyKey> drainAllRequests() {
        ArrayList<ABCSegmentReadyKey> requests = new ArrayList<>();
        readyKeyQueue.drainTo(requests);

        return requests;
    }

    public void emptyQueues() {
        retryQueue.clear();
        readyKeyQueue.clear();
    }

    @Scheduled(
        every = "{keyfinder.try.every.sec}",
        concurrentExecution = SKIP
    )
    void sendKafkaSeekLocations() {
        Log.debugf("scheduled retry - retryQueue(%s), readyKeyQueue(%s)", retryQueue.size(), readyKeyQueue.size());
        retryQueue.removeIf(tracker -> {
            if (tracker.readyKeysToRequest.isEmpty() || tracker.retries.get() >= maxCopyRetries) {
                retryDroppedEvent.fire(tracker);
                return true;
            } else {
                var locations = tracker.getNewSeekLocations(readyKeyCache);
                locations.forEach(loc -> readyKeyQueue.offer(loc));
                tracker.retries.incrementAndGet();
                return false;
            }
        });
    }
}
