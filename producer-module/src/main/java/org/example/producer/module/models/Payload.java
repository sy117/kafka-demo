package org.example.producer.module.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Payload {
    private int successCount;
    private int failedCount;
    private int retryAttempted;
}
