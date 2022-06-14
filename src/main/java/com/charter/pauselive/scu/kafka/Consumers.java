package com.charter.pauselive.scu.kafka;

import com.charter.pauselive.scu.service.KeyFinderCache;
import com.charter.pauselive.scu.service.ReadyKeyCache;
import com.charter.pauselive.scu.model.*;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vavr.collection.List;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.inject.Inject;

import static io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP;

/**
 * Consumer from main topics: segment-ready-keys and segment-copy-from-ready.
 * Consumers push messages to respective caches.
 */
@ApplicationScoped
public class Consumers {
    @ConfigProperty(name = "consumer.copyready.request.batch")
    Long copyReadyRequestBatch;

    @Inject
    KeyFinderCache keyFinderCache;
    @Inject
    ReadyKeyCache readyKeyCache;

    BaseSubscriber<PlayerCopyReady> copyReadySubscriber;

    @PostConstruct
    void init() {
        var fileProperties = List.ofAll(ConfigProvider.getConfig().getPropertyNames())
            .map(name -> ConfigProvider.getConfig().getConfigValue(name))
            .sortBy(configValue -> configValue.getSourceName() + configValue.getName())
            .filter(cfg -> !cfg.getName().equals("line.separator"))
            .map(cfg -> String.format("\n\t%s | %s =>\n\t\t%s", cfg.getSourceName(), cfg.getName(), cfg.getValue()));

        Log.debugf("\nConfigProvider INFO: %s", fileProperties);

        copyReadySubscriber = new BaseSubscriber<>() {
            @Override
            public void hookOnSubscribe(Subscription subscription) {
                request(1);
            }

            @Override
            public void hookOnNext(PlayerCopyReady message) {
                if (message.src().isBlank()) {
                    request(1);
                } else if (message.oldestSegment() < 1) {
                    Log.warnf("CopyReady message with invalid oldest segment: %s", message);
                } else
                    keyFinderCache.insert(message);
            }
        };
    }

    @Incoming("copy-ready-topic")
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public Subscriber<PlayerCopyReady> copyTopicConsumer() {
        return copyReadySubscriber;
    }

    @Incoming("ready-key-topic")
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> keyTopicConsumer(IncomingKafkaRecord<String, SegmentReadyKey> message) {
        var segmentReadyKey = message.getPayload();

        if (!segmentReadyKey.source().isBlank() &&
            segmentReadyKey.fallbackMessage().isPresent()) {
            Log.tracef("SegmentReadyKey consumer - cached: %s", message.getPayload());
            readyKeyCache.insert(message);
        }

        return CompletableFuture.completedFuture(null);
    }

    @Scheduled(
        every = "1s",
        concurrentExecution = SKIP
    )
    void pushCopyReadyRequests() {
        if (keyFinderCache.hasCapacity())
            copyReadySubscriber.request(copyReadyRequestBatch);
    }
}
