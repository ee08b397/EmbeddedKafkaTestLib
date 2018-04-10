package com.libkafkatest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Testing consumer that can send message(s) to an embedded Kafka broker.
 *
 * @author sozhang
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class TestingConsumer<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestingConsumer.class);

    private BlockingQueue<ConsumerRecord<K, V>> records;
    private KafkaConsumer<K, V> kafkaConsumer;

    public TestingConsumer(final KafkaEmbedded embeddedKafka, final Collection<String> topics) throws Exception {
        // Set up the Kafka consumer properties. Additional properties in KafkaSetup.
        final Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(
                KafkaConstants.GROUP_ID, "false", embeddedKafka);
        consumerProperties.put("auto.offset.reset", "earliest");

        kafkaConsumer = new KafkaConsumer<>(consumerProperties);
        kafkaConsumer.subscribe(topics);

        // Create a thread safe queue to store the received message.
        records = new LinkedBlockingQueue<>();

        kafkaConsumer.commitAsync((offsets, e) -> {
            if (e != null) {
                LOGGER.error("Testing consumer had error with " +
                                offsets.entrySet().stream()
                                        .map(p -> p.getKey() + " : " + p.getValue())
                                        .collect(Collectors.joining(", "))
                        , e);
            } else {
                LOGGER.debug("Testing consumer finished");
            }
        });

        final CountDownLatch latch = new CountDownLatch(10);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            try {
                while(true) {
                    final ConsumerRecords<K, V> records = kafkaConsumer.poll(1000);
                    for (ConsumerRecord<K, V> record : records) {
                        LOGGER.info("consuming from topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                        this.records.add(record);
                        latch.countDown();
                    }
                }
            } finally {
                kafkaConsumer.close();
            }
        });

        latch.await(10, TimeUnit.SECONDS);
    }

    /**
     * Close the consumer for tests, preferably at tear down.
     */
    public void closeConsumer() {
        this.kafkaConsumer.close();
    }

    public BlockingQueue<ConsumerRecord<K, V>> getRecords() {
        return records;
    }

}
