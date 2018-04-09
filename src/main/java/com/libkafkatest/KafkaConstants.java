package com.libkafkatest;

import org.springframework.kafka.test.rule.KafkaEmbedded;

/**
 * Overwritable system constants.
 *
 * @author sozhang
 */
public class KafkaConstants {

    /**
     * When the embedded Kafka server is started by JUnit, a system property
     * spring.embedded.kafka.brokers is set to the address of the Kafka broker(s).
     * Convenient constants KafkaEmbedded.SPRING_EMBEDDED_KAFKA_BROKERS is provided for this property.
     */
    final static String BOOTSTRAP_SERVERS = System.getProperty("KafkaTestingBootstrapServers", KafkaEmbedded.SPRING_EMBEDDED_KAFKA_BROKERS);

    final static String GROUP_ID = System.getProperty("KafkaTestingGroupID", "GroupID");

}
