package com.charter.pauselive.scu.kafka;

import com.charter.pauselive.scu.model.KafkaMetadata;
import com.charter.pauselive.scu.model.Payloads.*;
import com.charter.pauselive.scu.annot.EventTypes.SeekSuccess;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.KafkaProducer;
import io.vavr.collection.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.Fallback;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.reactive.messaging.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;
import java.time.Duration;

import static com.charter.pauselive.scu.service.Helpers.*;

@ApplicationScoped
public class SegmentReadyRouter {
    @ConfigProperty(name = "readyrouter.segmentready.topic")
    String segmentReadyTopic;
    @ConfigProperty(name = "readyrouter.poll.duration.milli")
    int pollMaxDuration;

    String readyRouterFinalTopicChannel = "segment-ready-topic-alt";
    String readyRouterFinalTopic;

    @Inject
    SeekerDispatcher seekerDispatcher;
    @Inject
    KafkaClientService clientService;

    @Inject
    @Channel("segment-ready-router")
    // TODO: action on many errors
    @OnOverflow(OnOverflow.Strategy.UNBOUNDED_BUFFER)
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
        readyRouterFinalTopic = ConfigProvider.getConfig().getValue(
            String.format("mp.messaging.outgoing.%s.topic", readyRouterFinalTopicChannel),
            String.class
        );
    }

    public void sendSeekRequest(ABCSegmentReadyKey request) {
        kafkaSeekEmitter.send(request);
    }

    @Retry(maxRetries = 3, delay = 500)
    @Fallback(fallbackMethod = "fetchSegmentMessageFallback")
    byte[] fetchSegmentReady(String topic, ABCSegmentReadyKey request) throws Exception {
        Log.tracef("fetchSegmentReady request: %s", request);
        byte[] rawMessage = new byte[0];
        var seeker = seekerDispatcher.getSeeker();

        try {
            rawMessage = List.ofAll(seeker.poll(Duration.ofMillis(pollMaxDuration)))
                .map(ConsumerRecord::value)
                .headOption()
                .getOrElse(new byte[0]);
        } catch (Exception e) {
            Log.errorf("Exception while polling: %s", e);
            e.printStackTrace();
        }

        rawMessage = new byte[0];
        seekerDispatcher.regainSeeker(seeker);
        if (rawMessage.length == 0) {
            Log.tracef("Request %s failed. Sending for retry...", request);
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
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public Uni<Void> trackerProcessor(Message<ABCSegmentReadyKey> message) {
        var readyKey = message.getPayload();
        var fallback = readyKey.fallbackMessage();
        KafkaProducer<String, byte[]> producer = clientService.getProducer(readyRouterFinalTopicChannel);
        Runnable sendFallback = () -> runIf(fallback.isPresent(), () -> fallbackEmitter.send(fallback.get()));

        var mono = Mono.just(readyKey)
            .flatMap(key -> Mono.fromCallable(() -> fetchSegmentReady(segmentReadyTopic, key))
                .doOnNext(bytes -> runIf(bytes.length == 0, sendFallback))
                .filter(bytes -> bytes.length != 0)
                .doOnNext(bytes -> producer.send(new ProducerRecord<>(readyRouterFinalTopic, bytes))
                    .subscribe().with(
                        meta -> Log.debugf(
                            "readyKey processed %s. Kafka broker response: %s", readyKey, KafkaMetadata.of(meta)
                        ),
                        e -> {
                            sendFallback.run();
                            Log.warnf("Producer got exception for %s. Fallback message sent... %s", readyKey, e);
                        }
                    ))
                .subscribeOn(Schedulers.boundedElastic())
            )
            .then();

        return asUni(mono);
    }

}
