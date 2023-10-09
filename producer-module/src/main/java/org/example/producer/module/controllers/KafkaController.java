package org.example.producer.module.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.example.producer.module.models.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
    @Autowired
    private ObjectMapper objectMapper;
    @Value("${kafka.producer.topic}")
    private String topicName;


    @PostMapping("/publish")
    public ResponseEntity<String> publishMessage(@RequestBody Message message){
        try {
            if(Objects.isNull(message.getId())){
                message.setId(UUID.randomUUID().toString());
            }
            if(Objects.isNull(message.getTimestamp())){
                message.setTimestamp(System.currentTimeMillis());
            }
            String jsonMessage = objectMapper.writeValueAsString(message);
            log.info("Publishing to topic: {}, msg: {}", topicName, jsonMessage);
            CompletableFuture<SendResult<String, String>> resp = kafkaTemplate.send(topicName, jsonMessage);
            resp.completeOnTimeout(null, 5, TimeUnit.SECONDS);
            return new ResponseEntity<>("Message published successfully", HttpStatus.OK);
        } catch (Exception ex){
            log.error(ex.getMessage());
        }
        return new ResponseEntity<>("Message publish failed", HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @GetMapping(value = "/status")
    public ResponseEntity<String> getStatus(){
        return new ResponseEntity<>("Status: UP", HttpStatus.OK);
    }

}
