package com.charter.pauselive.scu;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class GeneralTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
            "observer.debug.mode", "true",
            "mp.messaging.outgoing.segment-ready-sample-out.enabled", "false",
            "mp.messaging.outgoing.ready-key-sample-out.enabled", "false",
            "mp.messaging.outgoing.copy-from-sample-out.enabled", "false"
        );
    }
}
