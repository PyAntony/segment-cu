package com.charter.pauselive.scu.kafka;

import com.charter.pauselive.scu.model.Payloads.*;
import com.charter.pauselive.scu.annot.EventTypes.SeekSuccess;
import io.quarkus.logging.Log;
import io.vavr.collection.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.Fallback;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;
import java.time.Duration;

@ApplicationScoped
public class SegmentReadyRouter {
    @ConfigProperty(name = "readyrouter.segmentready.topic")
    String segmentReadyTopic;
    @ConfigProperty(name = "readyrouter.poll.duration.milli")
    int pollMaxDuration;

    @Inject
    SeekerDispatcher seekerDispatcher;
    @Inject
    @Channel("segment-ready-router")
    Emitter<ABCSegmentReadyKey> kafkaSeekEmitter;
    @Inject
    @SeekSuccess(value = true)
    Event<ABCSegmentReadyKey> seekEventSuccess;
    @Inject
    @SeekSuccess(value = false)
    Event<ABCSegmentReadyKey> seekEventFailure;

    @PostConstruct
    void prepareSeekers() {
        seekerDispatcher.assignSeekers(segmentReadyTopic);
    }

    public void sendSeekRequest(ABCSegmentReadyKey request) {
        kafkaSeekEmitter.send(request);
    }

    @Retry(maxRetries = 3, delay = 500)
    @Fallback(fallbackMethod = "fetchSegmentMessageFallback")
    byte[] fetchSegmentReady(String topic, ABCSegmentReadyKey request) throws Exception {
        var seeker = seekerDispatcher.getSeeker();
        byte[] rawMessage = new byte[0];

        try {
            rawMessage = List.ofAll(seeker.poll(Duration.ofMillis(pollMaxDuration)))
                .map(ConsumerRecord::value)
                .headOption()
                .getOrElse(new byte[0]);
        } catch (Exception e) {
            Log.errorf("Exception while polling: %s", e);
            e.printStackTrace();
        }

        seekerDispatcher.regainSeeker(seeker);
        if (rawMessage.length == 0) {
            Log.debugf("Request %s failed. Sending for retry...", request);
            throw new Exception("fetchCopyReadyMessage failed");
        }

        seekEventSuccess.fire(request);
        return rawMessage;
    }

    byte[] fetchSegmentMessageFallback(String topic, ABCSegmentReadyKey request) {
        seekEventFailure.fire(request);
        return new byte[0];
    }

    @Incoming("segment-ready-router")
    @Outgoing("segment-ready-topic-alt")
    public Flux<byte[]> trackerProcessor(Publisher<ABCSegmentReadyKey> kafkaLocations) {
        return Flux.from(kafkaLocations)
            .doOnNext(msg -> Log.tracef("Router - received request: %s", msg))
            .flatMap(req -> Mono.fromCallable(() -> fetchSegmentReady(segmentReadyTopic, req))
                .subscribeOn(Schedulers.boundedElastic())
            )
            .filter(bytes -> !(bytes.length == 0));
    }
}
