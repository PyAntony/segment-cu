package com.charter.pauselive.scu;

import com.charter.pauselive.scu.model.SegmentReadyKey;
import com.charter.pauselive.scu.service.ReadyKeyCache;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.providers.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.providers.connectors.InMemorySource;
import org.junit.jupiter.api.Test;

import javax.enterprise.inject.Any;
import javax.inject.Inject;
import java.util.Optional;


@QuarkusTest
@TestProfile(GeneralTestProfile.class)
@QuarkusTestResource(KafkaInMemoryResource.class)
public class ConsumerTest {
    //    // 1. Switch the channels to the in-memory connector:
//    @BeforeAll
//    public static void switchMyChannels() {
//        InMemoryConnector.switchIncomingChannelsToInMemory("copy-ready-topic");
//        InMemoryConnector.switchIncomingChannelsToInMemory("ready-key-topic");
//    }
//
//    // 2. Don't forget to reset the channel after the tests:
//    @AfterAll
//    public static void revertMyChannels() {
//        InMemoryConnector.clear();
//    }

//    @Inject
//    @Any
//    InMemoryConnector connector;
//    @Inject
//    ReadyKeyCache readyKeyCache;

//    @Test
//    void testReadyKeyConsumption() {
//        InMemorySource<IncomingKafkaRecord<String, SegmentReadyKey>> incomingReadyKeys =
//            connector.source("ready-key-topic");
//        // 4. Retrieves the in-memory source to send message
////        InMemorySource<Integer> prices = connector.source("copy-ready-topic");
////        // 5. Retrieves the in-memory sink to check what is received
////        InMemorySink<Integer> results = connector.sink("processed-prices");
//
//        // 6. Send fake messages:
////        prices.send(1);
////        prices.send(2);
////        prices.send(3);
//
//        // 7. Check you have receives the expected messages
////        Assertions.assertEquals(3, results.received().size());
//        incomingReadyKeys.send(
//            new IncomingKafkaRecord()
//            SegmentReadyKey.of("a", "b", 9, 9, 9, Optional.empty())
//        );
//
//        var readyKeyMap = readyKeyCache.getSourceSegmentMapView();
//        System.out.println(readyKeyMap);
//    }
}
