package com.libkafkatest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.hamcrest.KafkaMatchers;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertThat;

/**
 * Sample Unit test using an embedded Kafka broker.
 *
 * @author sozhang
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class SampleTest {

    private final static String sampleTopic = "sample.topic";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(2, true, sampleTopic, "t2", "t3");

    @Test
    public void testContainerReceive() throws Exception {
        final TestingTemplate testingTemplate = new TestingTemplate(embeddedKafka, Optional.of(sampleTopic));
        final TestingContainer testingContainer = new TestingContainer(embeddedKafka, sampleTopic);
        final KafkaTemplate<String, String> kafkaTemplate = testingTemplate.getKafkaTemplate();

        final String message = "Check this message is sent to consumer from producer. ";
        kafkaTemplate.sendDefault(message);

        try {
            final ConsumerRecord<String, String> record = testingContainer.getRecords().poll(1, TimeUnit.SECONDS);
            assertThat(record, KafkaMatchers.hasValue(message));
        } finally {
            testingContainer.stopContainer();
        }
    }

    @Test
    public void testConsumerReceive() throws Exception {
        final TestingProducer testingProducer = new TestingProducer(embeddedKafka);
        final TestingContainer testingContainer = new TestingContainer(embeddedKafka, sampleTopic);
        final KafkaProducer<String, String> producer = testingProducer.getProducer();

        final String message = "Check this message is sent to consumer from producer. ";
        producer.send(new ProducerRecord<>(sampleTopic, message)).get();

        try {
            final ConsumerRecord<String, String> record = testingContainer.getRecords().poll(1, TimeUnit.SECONDS);
            assertThat(record, KafkaMatchers.hasValue(message));
        } finally {
            testingContainer.stopContainer();
        }
    }

    @Test
    public void testProducerConsumerReceive() throws Exception {
        final TestingProducer testingProducer = new TestingProducer(embeddedKafka);
        final TestingConsumer<String, String> testingConsumer = new TestingConsumer<>(embeddedKafka,
                Collections.singletonList(sampleTopic));

        final KafkaProducer<String, String> producer = testingProducer.getProducer();

        final String message = "Check this message is sent to consumer from producer. ";
        producer.send(new ProducerRecord<>(sampleTopic, message)).get();

        final ConsumerRecord<String, String> record = testingConsumer.getRecords().poll(5, TimeUnit.SECONDS);
        assertThat(record, KafkaMatchers.hasValue(message));
    }

}