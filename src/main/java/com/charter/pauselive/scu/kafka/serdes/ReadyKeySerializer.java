package com.charter.pauselive.scu.kafka.serdes;

import com.charter.pauselive.scu.model.SegmentReadyKey;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;

public class ReadyKeySerializer extends ObjectMapperSerializer<SegmentReadyKey> {
}
