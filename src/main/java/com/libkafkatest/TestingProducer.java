package com.libkafkatest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Collection;
import java.util.Map;

/**
 * Testing producer that can send message(s) to an embedded Kafka broker.
 *
 * @author sozhang
 */
public class TestingProducer {

    private KafkaProducer<String, String> producer;

    public TestingProducer(final KafkaEmbedded embeddedKafka) throws Exception {
        // Set up the Kafka producer properties. Additional properties in KafkaSetup.
        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);

        // Create a Kafka producer
        producer = new KafkaProducer<>(senderProps);

        // Wait until the partitions are assigned.
        final Collection<MessageListenerContainer> listenerContainers = KafkaSetup.kafkaListenerEndpointRegistry
                .getListenerContainers();

        for (final MessageListenerContainer messageListenerContainer : listenerContainers) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafka.getPartitionsPerTopic());
        }
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

}
