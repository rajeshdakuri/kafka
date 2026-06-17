package com.example.kafkaProject.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class FraudAlert {
    private String userId;
    private String reason;
}
