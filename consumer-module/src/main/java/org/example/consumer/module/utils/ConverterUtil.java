package org.example.consumer.module.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.example.consumer.module.models.Message;
import org.example.consumer.module.models.Payload;

import java.io.DataInput;
import java.nio.charset.StandardCharsets;

@Slf4j
public class ConverterUtil {
    private static ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
    }


    public static Message convertStringToObject(String msg){
        Message message;
        try{
            message = objectMapper.readValue(msg.getBytes(StandardCharsets.UTF_8), Message.class);
            return message;
        } catch (Exception e) {
            log.error("Error converting string msg to model: {}", e.getMessage());
        }
        return null;
    }
}
