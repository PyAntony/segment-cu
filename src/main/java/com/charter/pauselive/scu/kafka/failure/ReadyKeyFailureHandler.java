package com.charter.pauselive.scu.kafka.failure;

import com.charter.pauselive.scu.model.*;
import io.quarkus.logging.Log;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import org.apache.kafka.common.header.Headers;

import javax.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;

@ApplicationScoped
@Identifier("ready-key-handler")
public class ReadyKeyFailureHandler implements DeserializationFailureHandler<SegmentReadyKey> {
    @Override
    public SegmentReadyKey decorateDeserialization(
        Uni<SegmentReadyKey> deserialization,
        String topic,
        boolean isKey,
        String deserializer,
        byte[] data,
        Headers headers
    ) {
        return deserialization
            .onFailure().recoverWithItem(() -> {
                String payload = new String(data, StandardCharsets.UTF_8);
                Log.warnf("Deserialization failed. Topic: %s, payload: %s", topic, payload);

                return SegmentReadyKey.of("", "", 0, 0, 0, Optional.empty());
            })
            .await().atMost(Duration.ofMillis(100));
    }
}
