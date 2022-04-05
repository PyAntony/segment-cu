package com.charter.pauselive.scu.kafka.failure;

import com.charter.pauselive.scu.model.PlayerCopyReady;
import io.quarkus.logging.Log;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import org.apache.kafka.common.header.Headers;

import javax.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

@ApplicationScoped
@Identifier("copy-ready-handler")
public class CopyReadyFailureHandler implements DeserializationFailureHandler<PlayerCopyReady> {
    @Override
    public PlayerCopyReady decorateDeserialization(
        Uni<PlayerCopyReady> deserialization,
        String topic,
        boolean isKey,
        String deserializer,
        byte[] data,
        Headers headers
    ) {
        // only called when failure hence `onItem()` won't work
        return deserialization
            .onFailure().recoverWithItem(() -> {
                String payload = new String(data, StandardCharsets.UTF_8);
                Log.warnf("Deserialization failed. Topic: %s, payload: %s", topic, payload);
                return PlayerCopyReady.of("", 0, 0);
            })
            .await().atMost(Duration.ofMillis(100));
    }
}
