package com.charter.pauselive.scu;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class IntegrationTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
            "observer.debug.mode", "true",
            "observer.history.track", "true",
            "activateSampleProducers", "true"
        );
    }
}
