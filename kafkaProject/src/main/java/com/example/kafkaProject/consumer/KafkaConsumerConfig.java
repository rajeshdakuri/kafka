package com.example.kafkaProject.consumer;
import com.example.kafkaProject.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {
    /**
     * The ConsumerFactory bean does NOT create a single consumer instance
     * It is not a consumer.
     * It is just a factory — a blueprint.
     * Spring uses it to create as many actual KafkaConsumer instances as needed at runtime.
     * One ConsumerFactory can produce unlimited consumers
     * You do not create multiple ConsumerFactory beans unless:
     * you want different groups
     * different deserializers
     * different bootstrap servers
     * different client configs
     * Otherwise, one ConsumerFactory is enough for hundreds of consumers.
     * @return
     */
    @Bean
    public ConsumerFactory<String, OrderEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-group");
        //we should pass class name to deserializer so that it will know target class to serialize
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JacksonJsonDeserializer<>(OrderEvent.class));
    }

}
