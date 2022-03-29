package com.charter.pauselive.scu.kafka;

import com.charter.pauselive.scu.service.RetryController;
import com.charter.pauselive.scu.service.ReadyKeyCache;
import com.charter.pauselive.scu.model.*;
import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.inject.Inject;

@ApplicationScoped
public class Consumers {
    @Inject
    RetryController retryController;
    @Inject
    ReadyKeyCache readyKeyCache;

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
                boolean capacityIsFull;
                do {
                    capacityIsFull = !retryController.insert(message);
                } while (capacityIsFull);

                Log.debugf("copyFrom consumer - message enqueued: %s", message.getPayload());
                request(1);
            }
        };
    }

    @Incoming("ready-key-topic")
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> keyTopicConsumer(IncomingKafkaRecord<String, SegmentReadyKey> message) {
        Log.tracef("received SegmentReadyKey: %s", message.getPayload());
        readyKeyCache.insert(message);

        return CompletableFuture.completedFuture(null);
    }
}
