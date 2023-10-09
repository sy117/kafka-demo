package org.example.consumer.module.services;

import lombok.extern.slf4j.Slf4j;
import org.example.consumer.module.models.Message;
import org.example.consumer.module.utils.ConverterUtil;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = "#{'${kafka.consumer.topic}'}", groupId = "#{'${kafka.consumer.groupId}'}")
    public void consume(String message){
        log.info("Message received: {}", message);
        Message kafkaMessage = ConverterUtil.convertStringToObject(message);
        int total = kafkaMessage.getPayload().getSuccessCount() + kafkaMessage.getPayload().getFailedCount();
        log.info("Total: {}", total);
    }
}
