package com.charter.pauselive.scu.kafka.sample;

import com.charter.pauselive.scu.model.*;
import io.quarkus.arc.Lock;
import io.quarkus.logging.Log;
import io.vavr.collection.List;
import lombok.AllArgsConstructor;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class SampleProducer {
    AtomicLong segmentTracker = new AtomicLong(1000);
    AtomicLong segmentReadyOffsetTracker = new AtomicLong(0);
    LinkedBlockingQueue<PlayerCopyReady> copyReadyPayloads = new LinkedBlockingQueue<>(Integer.MAX_VALUE);
    Optional<SegmentDownload> segmentDownload = Optional.of(SegmentDownload.of(
        SegmentReady.of("", "", "", Optional.of(UUID.randomUUID().toString()), ""),
        new ArrayList<>(), "", "", 42
    ));

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
        var encoding = Optional.of(UUID.randomUUID().toString());
        String source = "SRC-" + List.range(1, 99).shuffle().get(0);

        var profileMarkers = List.range(0, 4)
            .map(i -> new ProfileMarker(
                source,
                "profile" + i,
                segmentTracker.getAndIncrement(),
                segmentReadyOffsetTracker.getAndIncrement()
            ));

        var segmentReadyPayloads = profileMarkers.map(p ->
            SegmentReady.of(p.src, "???", "???", encoding, p.fileName())
        );
        var keyReadyPayloads = profileMarkers.map(p ->
            SegmentReadyKey.of(source, p.profile, p.segmentNumber, 0, p.offset, segmentDownload)
        );
        var playerCopy = PlayerCopyReady.of(
            source, profileMarkers.get(0).segmentNumber, profileMarkers.get(3).segmentNumber
        );

        keyReadyPayloads.forEach(key -> readyKeyEmitter.send(key));
        copyReadyPayloads.offer(playerCopy);

        return segmentReadyPayloads;
    }

    @AllArgsConstructor
    public static class ProfileMarker {
        String src;
        String profile;
        long segmentNumber;
        long offset;

        public String fileName() {
            return profile + "-" + segmentNumber + ".extension";
        }
    }
}
