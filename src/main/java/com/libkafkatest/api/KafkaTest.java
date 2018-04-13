package com.libkafkatest.api;

import com.libkafkatest.impl.KafkaTestImpl;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.util.Collection;
import java.util.Optional;

/**
 * Swappable library interface. See {@link KafkaTestImpl} for default implementation.
 *
 * @author sozhang
 */
public interface KafkaTest<K, V> {

    /**
     * Use default embedded broker
     *
     * @return producer for testing
     */
    Producer<K, V> getProducer() throws Exception;

    /**
     * Use given embedded broker
     *
     * @return producer for testing
     */
    Producer<K, V> getProducer(final KafkaEmbedded embeddedKafka) throws Exception;

    /**
     * Use default embedded broker
     *
     * @return consumer for testing
     */
    Consumer<K, V> getConsumer() throws Exception;

    /**
     * Use default embedded broker
     *
     * @return consumer for testing
     */
    Consumer<K, V> getConsumer(final Collection<String> topics) throws Exception;

    /**
     * Use given embedded broker
     *
     * @return consumer for testing
     */
    Consumer<K, V> getConsumer(final KafkaEmbedded embeddedKafka) throws Exception;

    /**
     * Use given embedded broker
     *
     * @return consumer for testing
     */
    Consumer<K, V> getConsumer(final KafkaEmbedded embeddedKafka, final Collection<String> topics) throws Exception;

    /**
     * Use default embedded broker
     *
     * @return kafka template for testing
     */
    KafkaTemplate<K, V> getTemplate() throws Exception;

    /**
     * Use given embedded broker
     *
     * @return kafka template for testing
     */
    KafkaTemplate<K, V> getTemplate(final KafkaEmbedded embeddedKafka) throws Exception;

    /**
     * Use given embedded broker
     *
     * @return kafka template for testing
     */
    KafkaTemplate<K, V> getTemplate(final Optional<String> defaultTopic) throws Exception;

    /**
     * Use given embedded broker
     *
     * @return kafka template for testing
     */
    KafkaTemplate<K, V> getTemplate(final KafkaEmbedded embeddedKafka, final Optional<String> defaultTopic) throws Exception;

    /**
     * Use default embedded broker
     *
     * @return kafka message listener container for testing
     */
    KafkaMessageListenerContainer<K, V> getContainer() throws Exception;

    /**
     * Use given embedded broker
     *
     * @return kafka message listener container for testing
     */
    KafkaMessageListenerContainer<K, V> getContainer(final KafkaEmbedded embeddedKafka) throws Exception;

    /**
     * Use default embedded broker
     *
     * @return kafka message listener container for testing
     */
    KafkaMessageListenerContainer<K, V> getContainer(final String... topics) throws Exception;

    /**
     * Use given embedded broker
     *
     * @return kafka message listener container for testing
     */
    KafkaMessageListenerContainer<K, V> getContainer(final KafkaEmbedded embeddedKafka, final String... topics) throws Exception;

}
