package com.charter.pauselive.scu.kafka;

import com.charter.pauselive.scu.service.RetryController;
import com.charter.pauselive.scu.service.ReadyKeyCache;
import com.charter.pauselive.scu.model.*;
import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vavr.collection.List;
import org.apache.commons.lang3.SerializationUtils;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.inject.Inject;

@ApplicationScoped
public class Consumers {
    @Inject
    RetryController retryController;
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
                if (message.getPayload().src().equals("ERROR")) {
                    request(1);
                    return;
                }

                boolean capacityIsFull;
                do {
                    capacityIsFull = !retryController.insert(message);
                } while (capacityIsFull);
                Log.tracef("playerCopyFrom consumer - message enqueued: %s", message.getPayload());

                request(1);
            }
        };
    }

    @Incoming("ready-key-topic")
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> keyTopicConsumer(IncomingKafkaRecord<String, SegmentReadyKey> message) {
        if (!message.getPayload().source().equals("ERROR")) {
            Log.tracef("SegmentReadyKey consumer - cached: %s", message.getPayload());
            readyKeyCache.insert(message);
        }

        return CompletableFuture.completedFuture(null);
    }
}
