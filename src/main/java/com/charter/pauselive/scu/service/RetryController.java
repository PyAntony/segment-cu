package com.charter.pauselive.scu.service;

import com.charter.pauselive.scu.kafka.SegmentReadyRouter;
import com.charter.pauselive.scu.model.*;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vavr.collection.List;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP;
import static io.vavr.API.For;


@ApplicationScoped
public class RetryController {
    @ConfigProperty(name = "retry.controller.queue.max")
    private int maxAllowedQueueCapacity;
    @ConfigProperty(name = "retry.controller.try.num")
    private int maxCopyRetries;

    private LinkedBlockingQueue<RetryTracker> retryTrackerQueue;

    @Inject
    ReadyKeyCache readyKeyCache;
    @Inject
    SegmentReadyRouter segmentReadyRouter;

    @Inject
    void setQueue() {
        retryTrackerQueue = new LinkedBlockingQueue<>(maxAllowedQueueCapacity);
    }

    public boolean insert(IncomingKafkaRecord<String, PlayerCopyReady> message) {
        return retryTrackerQueue.offer(new RetryTracker(message.getPayload(), readyKeyCache));
    }

    public void emptyQueue() {
        retryTrackerQueue.clear();
    }

    @Scheduled(
        every = "{retry.controller.try.every.sec}",
        concurrentExecution = SKIP
    )
    void sendKafkaSeekLocations() {
        retryTrackerQueue.removeIf(tracker -> {
            if (tracker.readyKeys.isEmpty() || tracker.retries.get() >= maxCopyRetries) {
                return true;
            } else {
                tracker.getSeekLocations(readyKeyCache).forEach(loc -> segmentReadyRouter.seekAndFetch(loc));
                tracker.retries.incrementAndGet();
                return false;
            }
        });
    }
}

class RetryTracker {
    List<ReadyKey> readyKeys;
    HashSet<Payloads.ABCProfileTracker> profilesSent;
    public AtomicInteger retries = new AtomicInteger(0);

    public RetryTracker(PlayerCopyReady copyReady, ReadyKeyCache cache) {
        long lastSegment = copyReady.lastProcessedSegment();
        long maxSegment = lastSegment > 0 ? lastSegment : estimateLastSegment(copyReady.oldestSegment(), cache);

        readyKeys = maxSegment > copyReady.oldestSegment() ?
            List.range(copyReady.oldestSegment(), maxSegment + 1)
                .map(segment -> ReadyKey.of(copyReady.src(), segment)) :
            List.empty();
    }

    public List<ReadyMeta> getSeekLocations(ReadyKeyCache cache) {
        var newLocations = For(readyKeys, key ->
            For(cache.getReadyLocations(key)
                .map(meta -> meta.zipWithTracker(key.source(), key.segmentNumber()))
                .filter(pair -> !profilesSent.contains(pair._1))
            ).yield()
        );

        return List.ofAll(newLocations)
            .asJava(list -> list.forEach(tuple -> profilesSent.add(tuple._1)))
            .map(tuple -> (ReadyMeta) tuple._2);
    }

    private long estimateLastSegment(long oldestSegment, ReadyKeyCache cache) {
        return cache.readyKeysOlderThan(oldestSegment)
            .map(Payloads.ABCReadyKey::segmentNumber)
            .max()
            .getOrElse(-1L);
    }
}
