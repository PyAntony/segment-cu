package com.charter.pauselive.scu.kafka.serdes;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;
import com.charter.pauselive.scu.model.*;

public class CopyReadyDeserializer extends ObjectMapperDeserializer<PlayerCopyReady> {
    public CopyReadyDeserializer() {
        super(PlayerCopyReady.class);
    }
}
