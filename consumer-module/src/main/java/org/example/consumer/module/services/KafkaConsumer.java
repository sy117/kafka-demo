package org.example.consumer.module.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = "#{'${kafka.consumer.topic}'}", groupId = "#{'${kafka.consumer.groupId}'}")
    public void consume(String message){
        log.info("Message received: {}", message);
    }
}
