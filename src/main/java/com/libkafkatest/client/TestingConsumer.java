package com.libkafkatest.client;

import com.libkafkatest.config.KafkaConstants;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Testing consumer that can send message(s) to an embedded Kafka broker.
 *
 * @implNote java.util.ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access
 * @author sozhang
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class TestingConsumer<K, V> {

    private final Consumer<K, V> kafkaConsumer;

    public TestingConsumer(final KafkaEmbedded embeddedKafka) throws Exception {
        kafkaConsumer = new KafkaConsumer<>(loadConfig(embeddedKafka));
        constructTestingConsumer(Collections.emptySet());
    }

    public TestingConsumer(final KafkaEmbedded embeddedKafka, final Collection<String> topics) throws Exception {
        kafkaConsumer = new KafkaConsumer<>(loadConfig(embeddedKafka));
        constructTestingConsumer(topics);
    }

    private void constructTestingConsumer(final Collection<String> topics) throws InterruptedException {
        synchronized (this.kafkaConsumer) {
            kafkaConsumer.subscribe(topics);
        }
    }

    private Map<String, Object> loadConfig(final KafkaEmbedded embeddedKafka) {
        // Set up the Kafka consumer properties. Additional properties in com.bfm.libkafkatest.KafkaSetup.
        return KafkaTestUtils.consumerProps(KafkaConstants.GROUP_ID, "false", embeddedKafka);
    }

    /**
     * Close the consumer for tests, preferably at tear down.
     */
    public void closeConsumer() {
        synchronized (this.kafkaConsumer) {
            this.kafkaConsumer.close();
        }
    }

    public Consumer<K, V> getKafkaConsumer() {
        return kafkaConsumer;
    }

}
