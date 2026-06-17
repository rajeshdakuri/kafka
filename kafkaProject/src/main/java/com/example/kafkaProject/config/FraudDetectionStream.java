package com.example.kafkaProject.config;

import com.example.kafkaProject.model.FraudAlert;
import com.example.kafkaProject.model.OrderEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.time.Duration;

@Configuration
public class FraudDetectionStream {

    /**
     * Builds a Kafka Streams topology that detects rapid-ordering behavior.
     * This stream:
     * - Consumes OrderEvent records from the "orders-topic".
     * - Groups records by key (user identifier).
     * - Applies 1‑minute time windows and counts how many orders each user places within each window.
     * - Filters out all windows except those where a user exceeds 5 orders.
     * - Converts those flagged windows into FraudAlert records.
     * - Publishes the resulting FraudAlert messages to the "fraud-alert-topic".
     * The returned KStream represents the raw incoming order stream.
     */
    @Bean
    public KStream<String, OrderEvent> stream(StreamsBuilder builder) {
        KStream<String, OrderEvent> stream = builder.stream("orders-topic");
        stream.groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count()
                .toStream()
                .filter((user, count) -> count > 5)
                .map((windowedKey, value) -> KeyValue.pair(windowedKey.key(), new FraudAlert(windowedKey.key(), "Too many orders in short time")))
                .to("fraud-alert-topic");
        return stream;
        //Kafka Streams topologies run in background threads and cannot return values to the frontend.
        //You must push results to:
        //Kafka topic + WebSocket
        //Queryable state store + REST
        //Database + UI polling
    }

    @Bean
    public StreamsBuilder builder() {
        return new StreamsBuilder();
    }
}
