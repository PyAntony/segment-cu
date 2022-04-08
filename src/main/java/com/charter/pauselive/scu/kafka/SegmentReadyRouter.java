package com.charter.pauselive.scu.kafka;

import com.charter.pauselive.scu.model.Payloads.*;
import com.charter.pauselive.scu.annot.EventTypes.SeekSuccess;
import com.charter.pauselive.scu.service.SeekEvent;

import com.charter.pauselive.scu.model.SegmentDownload;
import com.charter.pauselive.scu.service.KeyFinderCache;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.KafkaProducer;
import io.vavr.Tuple;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.charter.pauselive.scu.service.Helpers.*;
import static io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP;

@ApplicationScoped
public class SegmentReadyRouter {
    @ConfigProperty(name = "readyrouter.segmentready.topic")
    String segmentReadyTopic;

    AtomicInteger pendingRequests = new AtomicInteger(0);
    String readyRouterFinalTopicChannel = "segment-ready-topic-alt";
    String readyRouterFinalTopic;

    @Inject
    KeyFinderCache keyFinderCache;
    @Inject
    SeekerDispatcher seekerDispatcher;
    @Inject
    KafkaClientService clientService;

    @Inject
    @Channel("segment-ready-router")
    @OnOverflow(OnOverflow.Strategy.UNBOUNDED_BUFFER)
    Emitter<ABCSegmentReadyKey> kafkaSeekEmitter;

    @Inject
    @Channel("fallback-topic")
    Emitter<ABCSegmentDownload> fallbackEmitter;

    @Inject
    @SeekSuccess(value = true)
    Event<SeekEvent> seekEventSuccess;
    @Inject
    @SeekSuccess(value = false)
    Event<SeekEvent> seekEventFailure;

    @PostConstruct
    void prepareSeekers() {
        seekerDispatcher.assignSeekers(segmentReadyTopic);
        readyRouterFinalTopic = ConfigProvider.getConfig().getValue(
            String.format("mp.messaging.outgoing.%s.topic", readyRouterFinalTopicChannel),
            String.class
        );
    }

    @Scheduled(
        every = "1s",
        concurrentExecution = SKIP
    )
    void sendSeekRequest() {
        if (pendingRequests.get() <= seekerDispatcher.getAvailableSeekers()) {
            var requests = keyFinderCache.drainAllRequests();
            Log.debugf("seekerDispatcher in idle state. Sending %s requests...", requests.size());

            pendingRequests.getAndAdd(requests.size());
            requests.forEach(req -> kafkaSeekEmitter.send(req));
        } else
            Log.debugf("seekerDispatcher busy. Can't consume request. Pending: %s", pendingRequests.get());
    }

    @Retry(maxRetries = 3, delay = 500)
    @Fallback(fallbackMethod = "fetchSegmentMessageFallback")
    ConsumerRecord<String, byte[]> fetchSegmentReady(String topic, ABCSegmentReadyKey request) throws Exception {
        Log.tracef("fetchSegmentReady request: %s", request);
        var record = seekerDispatcher.search(segmentReadyTopic, request.partition(), request.offset());

        if (record.value().length == 0) {
            Log.tracef("Request %s failed. Sending for retry...", request);
            throw new Exception("fetchCopyReadyMessage failed!");
        } else
            return record;
    }

    ConsumerRecord<String, byte[]> fetchSegmentMessageFallback(String topic, ABCSegmentReadyKey request) {
        return seekerDispatcher.getEmptyRecord();
    }

    @Incoming("segment-ready-router")
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public Uni<Void> trackerProcessor(Message<ABCSegmentReadyKey> message) {
        var readyKey = message.getPayload();
        var fallback = readyKey.fallbackMessage();
        KafkaProducer<String, byte[]> producer = clientService.getProducer(readyRouterFinalTopicChannel);
        Consumer<String> onFailure = s -> processFailure(s, readyKey, fallback);

        var monoRecord = Mono.just(readyKey)
            .flatMap(key -> Mono.fromCallable(() -> fetchSegmentReady(segmentReadyTopic, key))
                .doOnNext(__ -> pendingRequests.decrementAndGet())
                .doOnNext(bytes -> runIf(
                    bytes.value().length == 0,
                    () -> onFailure.accept("SegmentReady not found"))
                )
                .filter(bytes -> bytes.value().length != 0)
                .subscribeOn(Schedulers.boundedElastic())
            );

        var monoVoid = monoRecord
            .flatMap(record -> asMono(
                producer.send(new ProducerRecord<>(readyRouterFinalTopic, record.value()))
                    .map(meta -> Tuple.of(record, meta))
            ))
            .onErrorContinue((e, obj) -> onFailure.accept("Producer exception: " + e))
            .doOnNext(tuple -> seekEventSuccess.fire(new SeekEvent(readyKey, tuple._1, tuple._2)))
            .then();

        return asUni(monoVoid);
    }

    private void processFailure(String event, ABCSegmentReadyKey request, Optional<SegmentDownload> fallback) {
        seekEventFailure.fire(new SeekEvent(request, null, null));
        runIf(fallback.isPresent(), () -> fallbackEmitter.send(fallback.get()));
        Log.warnf("%s - readyKey: %s, fallback sent: %s", event, request, fallback);
    }
}
