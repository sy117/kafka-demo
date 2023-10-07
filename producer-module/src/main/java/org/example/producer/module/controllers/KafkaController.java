package org.example.producer.module.controllers;

import lombok.extern.slf4j.Slf4j;
import org.example.producer.module.models.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@RestController
@Slf4j
public class KafkaController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private String topicName = "test-topic";

    @PostMapping("/publish")
    public ResponseEntity<String> publishMessage(@RequestBody Message message){
        try {
            if(Objects.isNull(message.getId())){
                message.setId(UUID.randomUUID().toString());
            }
            if(Objects.isNull(message.getTimestamp())){
                message.setTimestamp(System.currentTimeMillis());
            }
            log.info("Publishing to topic: {}, msg: {}", topicName, message);
            CompletableFuture<SendResult<String, String>> resp = kafkaTemplate.send(topicName, String.valueOf(message));
            resp.completeOnTimeout(null, 5, TimeUnit.SECONDS);
            return new ResponseEntity<>("Message published successfully", HttpStatus.OK);
        } catch (Exception ex){
            log.error(ex.getMessage());
        }
        return new ResponseEntity<>("Message published failed", HttpStatus.EXPECTATION_FAILED);
    }

    @GetMapping(value = "/status")
    public ResponseEntity<String> getStatus(){
        return new ResponseEntity<>("Status: UP", HttpStatus.OK);
    }

}
