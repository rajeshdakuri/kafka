package com.example.kafkaProject.consumer;


import com.example.kafkaProject.model.FraudAlert;
import com.example.kafkaProject.model.OrderEvent;
import com.example.kafkaProject.producer.DLQProducer;
import com.example.kafkaProject.producer.FraudAlertProducer;
import com.example.kafkaProject.service.OrderProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderConsumer {
    private final DLQProducer dlqProducer;
    private final FraudAlertProducer fraudAlertProducer;
    private final OrderProcessingService orderProcessingService;
    private final int retries = 3;

    /**
     * internally the below method uses the bean we have configured i.e.., consumer factory bean
     */
    @KafkaListener(topics = "order-topic", groupId = "order-group")
    private void consume(OrderEvent event, Acknowledgment ack) {
        for (int i = 1; i <= retries; i++) {
            try {
                if(detectFraud(event)) {
                   break;
                }
                orderProcessingService.process(event);
                ack.acknowledge();
                return;
            } catch (Exception e) {
                log.info(" retry {} failed", i);
                if (i == retries) {
                    dlqProducer.sendToDlq(event);
                }
            }
        }
        ack.acknowledge();
    }

    private boolean detectFraud(OrderEvent orderEvent) {
        if (orderEvent.getAmount() > 100000) {
            fraudAlertProducer.send(new FraudAlert(orderEvent.getUserId(), "HighValue Order Detected"));
            return true;
        }
        return false;
    }
}
