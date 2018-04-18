package com.libkafkatest;

import com.libkafkatest.api.KafkaTest;
import com.libkafkatest.client.TestingContainer;
import com.libkafkatest.impl.KafkaTestImpl;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.hamcrest.KafkaMatchers;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertThat;

/**
 * Sample Unit test using an embedded Kafka broker.
 *
 * @author sozhang
 */
public class SampleTest {

    private final static String sampleTopic = "sample.topic";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(2, true, sampleTopic, "t2", "t3");

    @Test
    public void testProducerConsumer() throws Exception {
        final KafkaTest<String, String> kafkaTest = new KafkaTestImpl<>();
        final Producer<String, String> producer = kafkaTest.getProducer(embeddedKafka);
        final Consumer<String, String> consumer = kafkaTest.getConsumer(embeddedKafka,
                Collections.singletonList(sampleTopic));

        final String message = "Check this message is sent to consumer from producer. ";
        producer.send(new ProducerRecord<>(sampleTopic, message)).get();

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            try {
                while (true) {
                    final ConsumerRecords<String, String> records;
                    synchronized (consumer) {
                        try {
                            records = consumer.poll(10);
                        } catch (IllegalStateException e) {
                            if (!e.getMessage().contains("This consumer has already been closed.")) {
                                throw e;
                            }

                            break;
                        }
                    }
                    records.forEach(record -> assertThat(record, KafkaMatchers.hasValue(message)));
                }
            } finally {
                consumer.close();
            }
        });

        synchronized (consumer) {
            consumer.close();
        }
    }

    @Test
    public void testProducerContainer() throws Exception {
        final KafkaTest<Integer, String> kafkaTest = new KafkaTestImpl<>();
        final Producer<Integer, String> producer = kafkaTest.getProducer(embeddedKafka);
        final TestingContainer<Integer, String> testingContainer = new TestingContainer<>(embeddedKafka, sampleTopic);

        final String message = "Check this message is sent to consumer from producer. ";
        producer.send(new ProducerRecord<>(sampleTopic, 1, message)).get();

        try {
            final ConsumerRecord<Integer, String> record = testingContainer.getRecords().poll(1, TimeUnit.SECONDS);
            assertThat(record, KafkaMatchers.hasValue(message));
        } finally {
            testingContainer.stopContainer();
        }
    }

    @Test
    public void testTemplateConsumer() throws Exception {
        final KafkaTest<Integer, String> kafkaTest = new KafkaTestImpl<>();
        final KafkaTemplate<Integer, String> kafkaTemplate = kafkaTest.getTemplate(embeddedKafka, Optional.of(sampleTopic));
        final Consumer<Integer, String> consumer = kafkaTest.getConsumer(embeddedKafka,
                Collections.singletonList(sampleTopic));

        final String message = "Check this message is sent to consumer from producer. ";
        kafkaTemplate.sendDefault(message);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            try {
                while (true) {
                    final ConsumerRecords<Integer, String> records;
                    synchronized (consumer) {
                        try {
                            records = consumer.poll(10);
                        } catch (IllegalStateException e) {
                            if (!e.getMessage().contains("This consumer has already been closed.")) {
                                throw e;
                            }

                            break;
                        }
                    }

                    records.forEach(record -> assertThat(record, KafkaMatchers.hasValue(message)));
                }
            } finally {
                consumer.close();
            }
        });

        synchronized (consumer) {
            consumer.close();
        }
    }

    @Test
    public void testTemplateContainer() throws Exception {
        final KafkaTest<String, String> kafkaTest = new KafkaTestImpl<>();
        final KafkaTemplate<String, String> kafkaTemplate = kafkaTest.getTemplate(embeddedKafka, Optional.of(sampleTopic));
        final TestingContainer<String, String> testingContainer = new TestingContainer<>(embeddedKafka, sampleTopic);

        final String message = "Check this message is sent to consumer from producer. ";
        kafkaTemplate.sendDefault(message);

        try {
            final ConsumerRecord<String, String> record = testingContainer.getRecords().poll(1, TimeUnit.SECONDS);
            assertThat(record, KafkaMatchers.hasValue(message));
        } finally {
            testingContainer.stopContainer();
        }
    }

}
