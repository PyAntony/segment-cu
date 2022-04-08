package com.charter.pauselive.scu.kafka;

import com.charter.pauselive.scu.service.KeyFinderCache;
import com.charter.pauselive.scu.service.ReadyKeyCache;
import com.charter.pauselive.scu.model.*;
import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vavr.collection.List;
import org.eclipse.microprofile.config.ConfigProvider;
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

@ApplicationScoped
public class Consumers {
    @Inject
    KeyFinderCache keyFinderCache;
    @Inject
    ReadyKeyCache readyKeyCache;

    @PostConstruct
    void LogConfig() {
        var fileProperties = List.ofAll(ConfigProvider.getConfig().getPropertyNames())
            .map(name -> ConfigProvider.getConfig().getConfigValue(name))
            .sortBy(configValue -> configValue.getSourceName() + configValue.getName())
            .filter(cfg -> !cfg.getName().equals("line.separator"))
            .map(cfg -> String.format("\n\t%s | %s =>\n\t\t%s", cfg.getSourceName(), cfg.getName(), cfg.getValue()));

        Log.debugf("\nConfigProvider INFO: %s", fileProperties);
    }

    @Incoming("copy-ready-topic")
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public Subscriber<IncomingKafkaRecord<String, PlayerCopyReady>> copyTopicConsumer() {
        return new BaseSubscriber<>() {
            @Override
            public void hookOnSubscribe(Subscription subscription) {
                request(1);
            }

            @Override
            public void hookOnNext(IncomingKafkaRecord<String, PlayerCopyReady> message) {
                if (message.getPayload().src().isBlank()) {
                    request(1);
                    return;
                }

                boolean capacityIsFull;
                do {
                    capacityIsFull = !keyFinderCache.insert(message);
                } while (capacityIsFull);
                Log.tracef("playerCopyFrom consumer - message enqueued: %s", message.getPayload());

                request(1);
            }
        };
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
}
