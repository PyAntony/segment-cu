package com.charter.pauselive.scu.kafka.serdes;

import com.charter.pauselive.scu.model.SegmentReady;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;

public class SegmentReadySerializer extends ObjectMapperSerializer<SegmentReady> {
}
