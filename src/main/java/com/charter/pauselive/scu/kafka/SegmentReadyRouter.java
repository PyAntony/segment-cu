package com.charter.pauselive.scu.kafka;

import com.charter.pauselive.scu.model.Payloads.*;
import com.charter.pauselive.scu.annot.EventTypes.SeekSuccess;
import com.charter.pauselive.scu.service.SeekEvent;

import com.charter.pauselive.scu.service.KeyFinderCache;
import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;
import java.time.Duration;

import static com.charter.pauselive.scu.service.Helpers.*;

/**
 * Class process each segment ready key: it requests the segmentReady message for that key,
 * send that message to the destination topic, and sends a fallback message when failure.
 */
@ApplicationScoped
public class SegmentReadyRouter {
    final String finalDestinationChannel = "segment-ready-topic-alt";
    String readyRouterFinalTopic;

    @Inject
    KeyFinderCache keyFinderCache;
    @Inject
    SeekerDispatcher seekerDispatcher;
    @Inject
    KafkaClientService clientService;

    @Inject
    @SeekSuccess(value = true)
    Event<SeekEvent> seekEventSuccess;
    @Inject
    @SeekSuccess(value = false)
    Event<SeekEvent> seekEventFailure;

    @PostConstruct
    void init() {
        readyRouterFinalTopic = ConfigProvider.getConfig().getValue(
            String.format("mp.messaging.outgoing.%s.topic", finalDestinationChannel),
            String.class
        );
    }

    @Outgoing("fallback-topic")
    public Flux<ABCSegmentDownload> generate() {
        return Flux.interval(Duration.ofSeconds(1))
            .flatMapIterable(__ -> keyFinderCache.drainAllRequests())
            .onBackpressureBuffer()
            .map(readyKey -> new SeekEvent(readyKey, null, null))
            .flatMap(event -> requestKey(event.getReadyKey())
                .map(event::withRecord)
                .subscribeOn(Schedulers.boundedElastic())
            )
            .flatMap(event -> event.getRecord().value().length == 0 ?
                Mono.just(event) :
                asMono(clientService.getProducer(finalDestinationChannel)
                    .send(new ProducerRecord<>(readyRouterFinalTopic, event.getRecord().value()))
                    .map(event::withBrokerResponse))
                    .onErrorReturn(event)
            )
            .doOnNext(event -> (event.getBrokerResponse() == null ? seekEventFailure : seekEventSuccess).fire(event))
            .filter(event -> event.getBrokerResponse() == null)
            .mapNotNull(event -> event.getReadyKey().fallbackMessage().orElse(null));
    }

    private Mono<ConsumerRecord<String, byte[]>> requestKey(ABCSegmentReadyKey key) {
        return Mono.fromCallable(() -> seekerDispatcher.search(key.partition(), key.offset()));
    }
}
