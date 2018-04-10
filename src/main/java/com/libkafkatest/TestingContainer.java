package com.libkafkatest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Testing consumer that can send message(s) to an embedded Kafka broker.
 *
 * @author sozhang
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class TestingContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestingContainer.class);

    private KafkaMessageListenerContainer<String, String> container;

    private BlockingQueue<ConsumerRecord<String, String>> records;

    public TestingContainer(final KafkaEmbedded embeddedKafka, final String... topics) throws Exception {
        // Set up the Kafka consumer properties. Additional properties in KafkaSetup.
        final Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(
                KafkaConstants.GROUP_ID, "false", embeddedKafka);

        // Create a Kafka consumer factory.
        final DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProperties);

        // Set the topic that needs to be consumed.
        final ContainerProperties containerProperties = new ContainerProperties(topics);

        // Create a Kafka MessageListenerContainer.
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

        // Create a thread safe queue to store the received message.
        records = new LinkedBlockingQueue<>();

        // Setup a Kafka message listener.
        container.setupMessageListener((MessageListener<String, String>) record -> {
            LOGGER.debug("test-listener received message='{}'", record.toString());
            records.add(record);
        });

        // Start the container and underlying message listener.
        container.start();

        // wait until the container has the required number of assigned partitions
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
    }

    /**
     * Stop the container for tests, preferably at tear down.
     */
    public void stopContainer() {
        this.container.stop();
    }

    public BlockingQueue<ConsumerRecord<String, String>> getRecords() {
        return records;
    }

}
