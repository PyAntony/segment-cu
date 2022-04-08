package com.charter.pauselive.scu;

import com.charter.pauselive.scu.model.Payloads;
import com.charter.pauselive.scu.model.SegmentReady;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.quarkus.runtime.Quarkus;
import org.apache.commons.lang3.SerializationUtils;

import java.util.Optional;

@QuarkusMain
public class Main {
    public static void main(String[] args) {
        System.out.println("Running Quarkus main method...");
        Quarkus.run(args);

//        var segment = SegmentReady.of("a", "b", "c", Optional.empty(), "vvv");
//
//        byte[] data = SerializationUtils.serialize(segment);
//        System.out.println(data);
//
//        Payloads.ABCSegmentReady deserialized = SerializationUtils.deserialize(data);
//        System.out.println(deserialized);
    }
}
