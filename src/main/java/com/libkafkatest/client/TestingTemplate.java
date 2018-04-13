package com.libkafkatest.client;

import com.libkafkatest.config.KafkaSetup;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Testing producer that can send message(s) to an embedded Kafka broker.
 *
 * @author sozhang
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class TestingTemplate<K, V> {

    private KafkaTemplate<K, V> kafkaTemplate;

    public TestingTemplate(final KafkaEmbedded embeddedKafka) throws Exception {
        constructTestingProducer(embeddedKafka, Optional.empty());
    }

    public TestingTemplate(final KafkaEmbedded embeddedKafka, final Optional<String> defaultTopic) throws Exception {
        constructTestingProducer(embeddedKafka, defaultTopic);
    }

    private void constructTestingProducer(final KafkaEmbedded embeddedKafka, final Optional<String> defaultTopic)
            throws Exception {

        // Set up the Kafka producer properties. Additional properties in com.bfm.libkafkatest.KafkaSetup.
        final Map<String, Object> props = KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());

        // Create a Kafka producer factory.
        final ProducerFactory<K, V> producerFactory = new DefaultKafkaProducerFactory<>(props);

        // Create a Kafka template.
        kafkaTemplate = new KafkaTemplate<>(producerFactory);

        // Set the default topic to send to (where a topic is not provided).
        defaultTopic.ifPresent(s -> kafkaTemplate.setDefaultTopic(s));

        // Wait until the partitions are assigned.
        final Collection<MessageListenerContainer> listenerContainers = KafkaSetup.kafkaListenerEndpointRegistry
                .getListenerContainers();

        for (final MessageListenerContainer messageListenerContainer : listenerContainers) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafka.getPartitionsPerTopic());
        }
    }

    /**
     * Use the same kafkaTemplate for producer and consumer
     *
     * @return kafka broker to use between producer and consumer
     */
    public KafkaTemplate<K, V> getKafkaTemplate() {
        return kafkaTemplate;
    }

}
