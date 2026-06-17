package com.example.kafkaProject.service;

import com.example.kafkaProject.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderProcessingService {
    public void process(OrderEvent order) {
        if(order.getAmount()<0) {
            throw new RuntimeException("invalid order amount");
        }
        log.info("order processed");
    }
}
