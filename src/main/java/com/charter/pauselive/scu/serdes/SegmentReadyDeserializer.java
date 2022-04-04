package com.charter.pauselive.scu.serdes;

import com.charter.pauselive.scu.model.PlayerCopyReady;
import com.charter.pauselive.scu.model.SegmentDownload;
import com.charter.pauselive.scu.model.SegmentReady;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class SegmentReadyDeserializer extends ObjectMapperDeserializer<SegmentReady>  {
    public SegmentReadyDeserializer() {
        super(SegmentReady.class);
    }
}
