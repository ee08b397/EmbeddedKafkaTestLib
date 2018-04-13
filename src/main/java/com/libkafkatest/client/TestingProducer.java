package com.libkafkatest.client;

import com.libkafkatest.config.KafkaSetup;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collection;
import java.util.Map;

/**
 * Testing producer that can send message(s) to an embedded Kafka broker.
 *
 * @author sozhang
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class TestingProducer<K, V> {

    private Producer<K, V> producer;

    public TestingProducer(final KafkaEmbedded embeddedKafka) throws Exception {
        // Set up the Kafka producer properties. Additional properties in com.bfm.libkafkatest.KafkaSetup.
        final Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka);

        // Create a Kafka producer
        producer = new KafkaProducer<>(props);

        // Wait until the partitions are assigned.
        final Collection<MessageListenerContainer> listenerContainers = KafkaSetup.kafkaListenerEndpointRegistry
                .getListenerContainers();

        for (final MessageListenerContainer messageListenerContainer : listenerContainers) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafka.getPartitionsPerTopic());
        }
    }

    public Producer<K, V> getProducer() {
        return producer;
    }

}
