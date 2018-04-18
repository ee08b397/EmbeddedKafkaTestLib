[![Build Status](https://travis-ci.org/ee08b397/EmbeddedKafkaTestLib.svg?branch=master)](https://travis-ci.org/ee08b397/EmbeddedKafkaTestLib)

# EmbeddedKafkaTestLib

An in-memory embedded Kafka broker unit test library implemented with Spring Kafka. There are 4 clients can be used: 

- `org.apache.kafka.clients.producer.Producer`
- `org.apache.kafka.clients.consumer.Consumer`
- `org.springframework.kafka.core.KafkaTemplate`
- `org.springframework.kafka.listener.KafkaMessageListenerContainer`

Depending on how the project to test is implemented, `Producer` and `KafkaTemplate` are message senders, 
and `Consumer` and `KafkaMessageListenerContainer` are message receivers. They can be used interchangeably. 

# How to Use

There is a sample unit test in `com.bfm.libkafkatest.SampleTest`. 
E.g., in `com.bfm.libkafkatest.SampleTest.testProducerConsumer`

```java
KafkaTest<String, String> kafkaTest = new KafkaTestImpl<>();
Producer<String, String> producer = kafkaTest.getProducer(embeddedKafka);
Consumer<String, String> consumer = kafkaTest.getConsumer(embeddedKafka, Collections.singletonList(sampleTopic));
```
Created with the same embedded Kafka broker, the `producer` and `consumer`  created here can be 
used for unit tests to send and receive messages.   
