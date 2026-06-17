package com.example.kafkaProject.controller;

import com.example.kafkaProject.model.OrderEvent;
import com.example.kafkaProject.producer.OrderProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/orders")
public class OrderController {
    private final OrderProducer orderProducer;

    @PostMapping
    public String createOrder(@RequestBody OrderEvent order) {
        orderProducer.sendOrder(order);
        return "Order sent to kafka";
    }
}
