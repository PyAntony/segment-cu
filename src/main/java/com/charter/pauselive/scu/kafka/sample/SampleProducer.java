package com.charter.pauselive.scu.kafka.sample;

import com.charter.pauselive.scu.model.*;
import io.quarkus.arc.Lock;
import io.quarkus.logging.Log;
import io.vavr.collection.List;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class SampleProducer {
    AtomicLong segmentReadyOffsetTracker = new AtomicLong(0);
    LinkedBlockingQueue<PlayerCopyReady> copyReadyPayloads = new LinkedBlockingQueue<>(Integer.MAX_VALUE);
    SegmentDownload segmentDownload = SegmentDownload.of(
        SegmentReady.of("", "", "", Optional.empty(), ""),
        new ArrayList<>(), "", "", 42
    );

    @Inject
    @Channel("ready-key-sample-out")
    Emitter<SegmentReadyKey> readyKeyEmitter;

    @Outgoing("segment-ready-sample-out")
    public Flux<SegmentReady> producer1() {
        return Flux.interval(Duration.ofMillis(1000))
            .map(__ -> produceMessages())
            .concatMap(Flux::fromIterable)
            .doOnNext(payload -> Log.tracef("Producing SegmentReadyKey payload %s", payload));
    }

    @Outgoing("copy-from-sample-out")
    public Flux<PlayerCopyReady> producer2() {
        return Flux.interval(Duration.ofMillis(1000))
            .filter(__ -> !copyReadyPayloads.isEmpty())
            .map(__ -> copyReadyPayloads.poll())
            .doOnNext(payload -> Log.tracef("Producing PlayerCopyReady payload %s", payload));
    }

    @Lock
    List<SegmentReady> produceMessages() {
        var offset1 = segmentReadyOffsetTracker.getAndIncrement();
        var offset2 = segmentReadyOffsetTracker.getAndIncrement();
        var offset3 = segmentReadyOffsetTracker.getAndIncrement();
        var offset4 = segmentReadyOffsetTracker.getAndIncrement();
        String src = "SRC-" + offset1;

        var segmentReadyPayloads = List.of(
            SegmentReady.of(src, "???", "???", Optional.of("segment-" + offset1), "video1"),
            SegmentReady.of(src, "???", "???", Optional.of("segment-" + offset1), "audio1"),
            SegmentReady.of(src, "???", "???", Optional.of("segment-" + offset3), "video1"),
            SegmentReady.of(src, "???", "???", Optional.of("segment-" + offset3), "audio1")
        );

        var keyReadyPayloads = List.of(
            SegmentReadyKey.of(src, "video1", offset1, 0, offset1, segmentDownload),
            SegmentReadyKey.of(src, "audio1", offset1, 0, offset2, segmentDownload),
            SegmentReadyKey.of(src, "video1", offset3, 0, offset3, segmentDownload),
            SegmentReadyKey.of(src, "audio1", offset3, 0, offset4, segmentDownload)
        );

        keyReadyPayloads.forEach(key -> readyKeyEmitter.send(key));
        copyReadyPayloads.offer(PlayerCopyReady.of(src, offset1, offset4));

        return segmentReadyPayloads;
    }
}
