package com.charter.pauselive.scu.service;

import com.charter.pauselive.scu.model.KafkaMetadata;
import com.charter.pauselive.scu.model.KafkaRecordMeta;
import com.charter.pauselive.scu.model.Payloads.*;
import com.charter.pauselive.scu.model.SegmentReady;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Objects;

@AllArgsConstructor
public class SeekEvent {
    static final ObjectMapper objectMapper = new ObjectMapper();

    private ABCSegmentReadyKey readyKey;
    private ConsumerRecord<String, byte[]> record;
    private RecordMetadata brokerResponse;

    public String getRepr() {
        return String.format(
            "SeekEvent{readyKey=%s, segmentReadyMeta=%s, brokerResp=%s}",
            readyKey,
            KafkaRecordMeta.of(record),
            KafkaMetadata.of(brokerResponse)
        );
    }

    public String getDetailedRepr() {
        ABCSegmentReady segmentReady = asSegmentReadyObj(record.value());

        return String.format(
            "SeekEvent{readyKey=%s, segmentReadyMeta=%s, segmentReady=%s, brokerResp=%s, VALID=%s}",
            readyKey,
            KafkaRecordMeta.of(record),
            segmentReady,
            KafkaMetadata.of(brokerResponse),
            validateSeekEvent(segmentReady)
        );
    }

    @SneakyThrows
    private ABCSegmentReady asSegmentReadyObj(byte[] bytes) {
        objectMapper.registerModule(new Jdk8Module());
        return objectMapper.readValue(new String(bytes), SegmentReady.class);
    }

    private boolean validateSeekEvent(ABCSegmentReady segmentReady) {
        return readyKey.partition() == record.partition() &&
            readyKey.offset() == record.offset() &&
            Objects.equals(readyKey.source(), segmentReady.source()) &&
            segmentReady.fileName().contains(String.valueOf(readyKey.segmentNumber())) &&
            segmentReady.fileName().contains(readyKey.profile());
    }
}
