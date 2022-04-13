package com.charter.pauselive.scu.kafka.sample;

import com.charter.pauselive.scu.model.*;
import io.quarkus.logging.Log;
import io.vavr.collection.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static io.vavr.API.For;

@ApplicationScoped
public class SampleProducer {
    public static List<String> sources = List.of("SRC-11", "SRC-22", "SRC-33");
    public static List<String> profiles = List.of("vide1", "video2", "audio1");
    AtomicLong segmentNumberTracker = new AtomicLong(1000);
    SegmentDownload segmentDownload = SegmentDownload.of(
        SegmentReady.of("", "", "", Optional.empty(), ""),
        new ArrayList<>(), "", "", 42
    );

    @Outgoing("segment-ready-sample-out")
    public Flux<SegmentReady> segmentReadyProducer() {
        return Flux.interval(Duration.ofMillis(1000))
            .flatMapIterable(__ -> segmentReadyList(segmentNumberTracker.getAndIncrement()))
            .doOnNext(payload -> Log.tracef("Producing SegmentReady payload %s", payload));
    }

    @Incoming("segment-ready-sample-in")
    @Outgoing("ready-key-sample-out")
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public Flux<SegmentReadyKey> readyKeyProducer(Publisher<ConsumerRecord<String, SegmentReady>> segmentReadyStream) {
        return Flux.from(segmentReadyStream)
            .map(record -> {
                SegmentReady payload = record.value();
                String[] profileSegment = StringUtils.split(payload.fileName(), "::");

                return SegmentReadyKey.of(
                    payload.source(),
                    profileSegment[0],
                    Long.parseLong(profileSegment[1]),
                    record.partition(),
                    record.offset(),
                    Optional.of(segmentDownload)
                );
            })
            .doOnNext(payload -> Log.tracef("Producing SegmentReadyKey payload %s", payload));
    }

    @Outgoing("copy-from-sample-out")
    public Flux<PlayerCopyReady> segmentCopyFromProducer() {
        return Flux.interval(Duration.ofMillis(3000))
            .filter(__ -> segmentNumberTracker.get() > 1005)
            .map(__ -> {
                long latestOffset = segmentNumberTracker.get() - 1;
                return PlayerCopyReady.of(
                    sources.shuffle().get(0),
                    latestOffset - 2,
                    latestOffset
                );
            });
    }

    private List<SegmentReady> segmentReadyList(long segmentNumber) {
        var encoding = Optional.of(StringUtils.repeat(UUID.randomUUID().toString(), 2));

        return List.ofAll(For(sources, src ->
            For(profiles
                .map(profile -> String.format("%s::%s", profile, segmentNumber))
                .map(fileName -> SegmentReady.of(src, "???", "???", encoding, fileName))
            ).yield()
        ));
    }
}
