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
        // Just let the app run. Sample producers send payloads for app to process data.
        Log.debug("Sleeping before all tests...");
        Thread.sleep(30000);
    }

    /**
     * Test that, for each segment requested (from PlayerCopyReady), all profiles
     * have been found in readyKeyCache. Profiles sent in sample producers are deterministic,
     * so we know which profiles we expect for each source and segment.
     */
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

    /**
     * Test that each segmentReadyKey has been processed: each segmentReady has been found when seeked and
     * delivered to the final topic.
     */
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

    /**
     * Validate each segmentReadyKey processed. See `validateSeekEvent` in `SeekEvent` class
     * for validation conditions.
     */
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
