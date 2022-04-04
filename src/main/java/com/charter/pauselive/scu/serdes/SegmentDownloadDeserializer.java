package com.charter.pauselive.scu.serdes;

import com.charter.pauselive.scu.model.PlayerCopyReady;
import com.charter.pauselive.scu.model.SegmentDownload;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class SegmentDownloadDeserializer extends ObjectMapperDeserializer<SegmentDownload> {
    public SegmentDownloadDeserializer() {
        super(SegmentDownload.class);
    }
}
