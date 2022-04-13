package com.charter.pauselive.scu;

import com.charter.pauselive.scu.kafka.sample.SampleProducer;
import com.charter.pauselive.scu.service.RequestsTracker;
import com.charter.pauselive.scu.service.SCUObserver;
import com.charter.pauselive.scu.service.SeekEvent;
import io.quarkus.logging.Log;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.vavr.collection.HashSet;
import io.vavr.collection.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@QuarkusTestResource(
    value = KafkaCompanionResource.class,
    initArgs = {
        @ResourceArg(name = "kafka.port", value = "49156"),
        @ResourceArg(name = "num.partitions", value = "5")
    }
)
public class IntegrationTest {
    @Inject
    SCUObserver observer;

    @BeforeAll
    public static void setup() throws InterruptedException {
        // let app run for some time
        Log.debug("Sleeping before all tests...");
        Thread.sleep(30000);
    }

    @Test
    void testProfilesFoundInReadyKeyCache() {
        observer.getRequestTrackerHistory().values().forEach(
            tracker -> {
                var sets = tracker.getSets();
                var readyKeysSearched = sets._1;
                var segmentReadyKeysSent = sets._2;

                var formatted = segmentReadyKeysSent.map(k -> "\n\t" + k);
                Log.debugf("ReadyKeys %s. \nProfiles sent: %s", readyKeysSearched, formatted);

                assertEquals(
                    segmentReadyKeysSent.size(),
                    readyKeysSearched.size() * SampleProducer.profiles.size()
                );
            }
        );
    }

    @Test
    void testAllSegmentReadyKeysSuccessfullyProcessed() {
        var sentHistory = observer.getSuccessfulHistory();
        var trackers = List.ofAll(observer.getRequestTrackerHistory().values());

        var trackersNotSent = trackers.filter(t -> !sentHistory.containsKey(t.getCopyReadyRequestId()));
        var notSentIds = HashSet.ofAll(trackersNotSent.map(RequestsTracker::getCopyReadyRequestId));

        var failedProfiles = trackers
            .filter(t -> !notSentIds.contains(t.getCopyReadyRequestId()))
            .flatMap(t -> {
                var readyKeysSent = sentHistory.get(t.getCopyReadyRequestId()).map(SeekEvent::getReadyKey);
                return t.getSets()._2.removeAll(readyKeysSent);
            });

        Log.debugf("SegmentReadyKeys not sent: %s", failedProfiles.map(k -> "\n\t" + k));
        assertTrue(trackersNotSent.isEmpty());
        assertTrue(failedProfiles.isEmpty());
    }

    @Test
    void testAllSegmentReadyKeysAreValid() {
        var sentHistory = List.ofAll(observer.getSuccessfulHistory().values());

        var invalidSeekEvents = sentHistory
            .flatMap(List::ofAll)
            .map(seek -> seek.asInfoPayload(true))
            .filter(seek -> !seek.isValidated());

        Log.debugf("Invalid Seek events: %s", invalidSeekEvents.map(k -> "\n\t" + k));
        assertTrue(invalidSeekEvents.isEmpty());
    }
}
