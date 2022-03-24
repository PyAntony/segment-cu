package com.charter.pauselive.scu.serdes;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;
import com.charter.pauselive.scu.model.*;

public class ReadyKeyDeserializer extends ObjectMapperDeserializer<SegmentReadyKey> {
    public ReadyKeyDeserializer() {
        super(SegmentReadyKey.class);
    }
}
