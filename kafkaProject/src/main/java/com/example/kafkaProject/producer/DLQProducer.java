package com.example.kafkaProject.producer;

import com.example.kafkaProject.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class DLQProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendToDlq(OrderEvent event) {
        kafkaTemplate.send("orders-dlq", event.getUserId(), event);
        log.info("Message sent to DLQ");
    }
}
