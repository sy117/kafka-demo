package org.example.consumer.module.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Message {
    private String id;
    private String sender;
    private Long timestamp;
    private Payload payload;
}
