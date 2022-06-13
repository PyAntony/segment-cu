package com.charter.pauselive.scu;

import io.quarkus.runtime.annotations.QuarkusMain;
import io.quarkus.runtime.Quarkus;
import io.vavr.collection.HashSet;

@QuarkusMain
public class Main {
    public static void main(String[] args) {
        System.out.println("Running Quarkus main method...");
        Quarkus.run(args);
    }
}
