package com.libkafkatest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.hamcrest.KafkaMatchers;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

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
    public void testReceive() throws Exception {
        final TestingProducer testingProducer = new TestingProducer(embeddedKafka, Optional.of(sampleTopic));
        final TestingConsumer testingConsumer = new TestingConsumer(embeddedKafka, sampleTopic);
        final KafkaTemplate<String, String> kafkaTemplate = testingProducer.getKafkaTemplate();

        final String message = "Check this message is sent to consumer from producer. ";
        kafkaTemplate.sendDefault(message);

        final ConsumerRecord<String, String> record = testingConsumer.getRecords().poll(1, TimeUnit.SECONDS);
        assertThat(record, KafkaMatchers.hasValue(message));

        testingConsumer.stopContainer();
    }

}