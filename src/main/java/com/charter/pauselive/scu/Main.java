package com.charter.pauselive.scu;

import com.charter.pauselive.scu.model.ReadyMeta;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.quarkus.runtime.Quarkus;
import io.vavr.collection.HashSet;

import java.time.Instant;

@QuarkusMain
public class Main {
    public static void main(String[] args) {
        System.out.println("Running Quarkus main method...");
        Quarkus.run(args);
    }
}
