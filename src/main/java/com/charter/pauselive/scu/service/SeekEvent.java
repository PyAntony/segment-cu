package com.charter.pauselive.scu.service;

import com.charter.pauselive.scu.model.*;
import com.charter.pauselive.scu.model.Payloads.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import lombok.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Arrays;
import java.util.Objects;

@Value
@With
public class SeekEvent {
    static final ObjectMapper objectMapper = new ObjectMapper();

    ABCSegmentReadyKey readyKey;
    ConsumerRecord<String, byte[]> record;
    RecordMetadata brokerResponse;

    public SeekEventInfo asInfoPayload(boolean detailed) {
        SegmentReady segmentReady = detailed ? asSegmentReadyObj(record.value()) : null;
        String metrics = detailed ? getFetchMetrics(record) : "...";
        boolean valid = detailed ? validateSeekEvent(segmentReady) : false;

        return SeekEventInfo.builder()
            .readyKey(readyKey)
            .segmentReadyFoundMeta(KafkaRecordMeta.of(record))
            .segmentReadyFound(segmentReady)
            .brokerResponse(KafkaMetadata.of(brokerResponse))
            .fetchMetrics(metrics)
            .validated(valid)
            .build();
    }

    @SneakyThrows
    private SegmentReady asSegmentReadyObj(byte[] bytes) {
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

    private String getFetchMetrics(ConsumerRecord<String, ?> record) {
        return Arrays.stream(record.headers().toArray())
            .filter(h -> h.key().equals(SeekerFetchMetrics.key()))
            .map(h -> new String(h.value()))
            .findAny()
            .orElse("???");
    }

    @Value
    @Builder(toBuilder = true)
    @AllArgsConstructor
    public static class SeekEventInfo {
        ABCSegmentReadyKey readyKey;
        KafkaRecordMeta segmentReadyFoundMeta;
        SegmentReady segmentReadyFound;
        KafkaMetadata brokerResponse;
        String fetchMetrics;
        boolean validated;
    }
}
