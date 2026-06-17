package com.example.kafkaProject.model;

import lombok.Data;

@Data
public class OrderEvent {
    private String orderId;
    private String userId;
    private Double amount;
    private Long timestamp;
}
