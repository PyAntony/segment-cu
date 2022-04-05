package com.charter.pauselive.scu.kafka;

import com.charter.pauselive.scu.model.Payloads.*;
import com.charter.pauselive.scu.annot.EventTypes.SeekSuccess;

import io.quarkus.logging.Log;
import io.vavr.collection.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.Fallback;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.reactive.messaging.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static com.charter.pauselive.scu.service.Helpers.runIf;

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
    @Channel("fallback-topic")
    Emitter<ABCSegmentDownload> fallbackEmitter;
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
            Log.tracef("Request %s failed. Sending for retry...", request);
            throw new Exception("fetchCopyReadyMessage failed");
        }

        seekEventSuccess.fire(request);
        return rawMessage;
    }

    byte[] fetchSegmentMessageFallback(String topic, ABCSegmentReadyKey request) {
        var fallback = request.fallbackMessage();

        seekEventFailure.fire(request);
        runIf(fallback.isPresent(), () -> fallbackEmitter.send(fallback.get()));
        return new byte[0];
    }

    @Incoming("segment-ready-router")
    @Outgoing("segment-ready-topic-alt")
    public Flux<Message<byte[]>> trackerProcessor(Publisher<Message<ABCSegmentReadyKey>> kafkaLocations) {
        return Flux.from(kafkaLocations)
            .doOnNext(msg -> Log.tracef("Router - received request: %s", msg.getPayload()))
            .flatMap(reqMsg -> processSegmentReadyKey(reqMsg)
                .subscribeOn(Schedulers.boundedElastic())
            );
    }

    private Mono<Message<byte[]>> processSegmentReadyKey(Message<ABCSegmentReadyKey> message) {
        var readyKey = message.getPayload();
        return Mono.fromCallable(() -> fetchSegmentReady(segmentReadyTopic, readyKey))
            .filter(bytes -> bytes.length != 0)
            .map(bytes ->
                Message.of(bytes).withAck(() -> {
                        Log.debugf("SegmentReadyKey successfully processed: %s", readyKey);
                        return message.getAck().get();
                    })
                    .withNack(e -> {
                        Log.warnf("SegmentReady fetched %s, but producer signal error: %s", readyKey, e);
                        return CompletableFuture.completedFuture(null);
                    })
            );
    }
}
