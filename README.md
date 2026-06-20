# Kafka Order Processing & Fraud Detection System

## Overview

A Spring Boot application demonstrating an event-driven architecture using Apache Kafka and Kafka Streams.

The system:

* Accepts customer orders through a REST API.
* Publishes orders to Kafka.
* Processes orders asynchronously using Kafka Consumers.
* Detects fraudulent activities.
* Sends failed messages to a Dead Letter Queue (DLQ).
* Generates fraud alerts in real time.

## Technologies

* Java
* Spring Boot
* Apache Kafka
* Kafka Streams
* Spring Kafka
* Lombok
* Maven

## Architecture

```text
Client
  |
  v
OrderController
  |
  v
OrderProducer
  |
  v
orders-topic
  |
  +-------------------+
  |                   |
  v                   v
OrderConsumer   Kafka Streams
  |                   |
  v                   v
Process Order   Fraud Detection
  |                   |
  +-------> DLQ       |
                      v
             fraud-alert-topic
```

## Features

* Asynchronous order processing
* Manual offset acknowledgment
* Retry mechanism (3 attempts)
* Dead Letter Queue (DLQ)
* High-value order fraud detection
* Real-time fraud detection using Kafka Streams
* Idempotent Kafka producer configuration

## API

### Create Order

```http
POST /orders
```

Request:

```json
{
  "orderId": "ORD-1001",
  "userId": "USER-1",
  "amount": 5000,
  "timestamp": 1745000000
}
```

Response:

```text
Order sent to kafka
```

## Kafka Topics

| Topic             | Purpose         |
| ----------------- | --------------- |
| orders-topic      | Incoming orders |
| fraud-alert-topic | Fraud alerts    |
| orders-dlq        | Failed orders   |

## Running the Project

Start Kafka and create the required topics:

```bash
orders-topic
fraud-alert-topic
orders-dlq
```

Run the application:

```bash
mvn spring-boot:run
```

## Concepts Demonstrated

* Kafka Producer & Consumer
* Consumer Groups
* Manual Offset Commit
* Kafka Streams
* Dead Letter Queue (DLQ)
* Retry Handling
* Event-Driven Architecture
* Fraud Detection Pipeline
* Spring Boot + Kafka Integration

Author

Rajesh Dakuri
```
```
