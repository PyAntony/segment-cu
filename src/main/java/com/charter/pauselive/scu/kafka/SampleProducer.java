package com.charter.pauselive.scu.kafka;

import com.charter.pauselive.scu.model.SegmentReadyKey;
import io.quarkus.logging.Log;
import io.vavr.collection.List;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import reactor.core.publisher.Flux;

import javax.enterprise.context.ApplicationScoped;
import java.time.Duration;

@ApplicationScoped
public class SampleProducer {
    static List<String> sources = List.range(1, 13).map(i -> "Src-" + i);
    static List<String> profiles = List.of("vid1", "vid2", "vid3", "vid4", "audio1");

    @Outgoing("ready-key-out")
    public Flux<SegmentReadyKey> producer1() {
        return Flux.interval(Duration.ofMillis(500))
            .map(__ -> SegmentReadyKey.of(
                sources.shuffle().get(0),
                profiles.shuffle().get(0),
                System.currentTimeMillis() / 1000,
                List.range(1, 11).shuffle().get(0),
                System.currentTimeMillis() / 1000000
            ))
            .doOnNext(payload -> Log.tracef("Producing segment agg payload %s", payload));
    }
}
