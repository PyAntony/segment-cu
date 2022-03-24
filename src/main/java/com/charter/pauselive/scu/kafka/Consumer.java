package com.charter.pauselive.scu.kafka;

import com.charter.pauselive.scu.cache.ReadyKeyCache;
import com.charter.pauselive.scu.model.*;
import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.inject.Inject;

@ApplicationScoped
public class Consumer {
    @Inject
    ReadyKeyCache readyKeyCache;

    @Incoming("ready-key-topic")
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> readyTopicConsumer(IncomingKafkaRecord<String, SegmentReadyKey> message) {
        Log.tracef("received SegmentReadyKey: %s", message.getPayload());
        readyKeyCache.insert(message);

        return CompletableFuture.completedFuture(null);
    }

//    @Incoming("copy-ready-topic")
//    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
//    public CompletionStage<Void> copyReadyTopicConsumer(IncomingKafkaRecord<String, PlayerCopyReady> message) {
//        Log.tracef("received PlayerCopyReady: %s", message.getPayload());
////        readyKeyCache.insert(message);
//
//        return message.ack();
//    }
}
