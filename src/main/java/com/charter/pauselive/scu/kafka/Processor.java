package com.charter.pauselive.scu.kafka;

import io.smallrye.reactive.messaging.kafka.KafkaClientService;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class Processor {

    @Inject
    KafkaClientService clientService;
}
