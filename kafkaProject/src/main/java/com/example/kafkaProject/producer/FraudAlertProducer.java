package com.example.kafkaProject.producer;

import com.example.kafkaProject.model.FraudAlert;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudAlertProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void send(FraudAlert fraudAlert) {
        kafkaTemplate.send("fraud-alert-topic", fraudAlert.getUserId(),fraudAlert);
        log.info("message sent fraud-alert-topic");
    }
}
