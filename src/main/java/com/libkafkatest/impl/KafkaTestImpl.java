package com.libkafkatest.impl;

import com.libkafkatest.api.KafkaTest;
import com.libkafkatest.client.TestingConsumer;
import com.libkafkatest.client.TestingContainer;
import com.libkafkatest.client.TestingProducer;
import com.libkafkatest.client.TestingTemplate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.ClassRule;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.util.Collection;
import java.util.Optional;

public class KafkaTestImpl<K, V> implements KafkaTest<K, V> {

    @ClassRule
    public static KafkaEmbedded defaultEmbeddedKafka = new KafkaEmbedded(2);

    @Override
    public Producer<K, V> getProducer() throws Exception {
        return new TestingProducer<K, V>(defaultEmbeddedKafka).getProducer();
    }

    @Override
    public Producer<K, V> getProducer(final KafkaEmbedded embeddedKafka) throws Exception {
        return new TestingProducer<K, V>(embeddedKafka).getProducer();
    }

    @Override
    public Consumer<K, V> getConsumer() throws Exception {
        return new TestingConsumer<K, V>(defaultEmbeddedKafka).getKafkaConsumer();
    }

    @Override
    public Consumer<K, V> getConsumer(final Collection<String> topics) throws Exception {
        return new TestingConsumer<K, V>(defaultEmbeddedKafka, topics).getKafkaConsumer();
    }

    @Override
    public Consumer<K, V> getConsumer(final KafkaEmbedded embeddedKafka) throws Exception {
        return new TestingConsumer<K, V>(embeddedKafka).getKafkaConsumer();
    }

    @Override
    public Consumer<K, V> getConsumer(final KafkaEmbedded embeddedKafka, final Collection<String> topics)
            throws Exception {

        return new TestingConsumer<K, V>(embeddedKafka, topics).getKafkaConsumer();
    }

    @Override
    public KafkaTemplate<K, V> getTemplate() throws Exception {
        return new TestingTemplate<K, V>(defaultEmbeddedKafka).getKafkaTemplate();
    }

    @Override
    public KafkaTemplate<K, V> getTemplate(final KafkaEmbedded embeddedKafka) throws Exception {
        return new TestingTemplate<K, V>(embeddedKafka).getKafkaTemplate();
    }

    @Override
    public KafkaTemplate<K, V> getTemplate(final Optional<String> defaultTopic) throws Exception {
        return new TestingTemplate<K, V>(defaultEmbeddedKafka, defaultTopic).getKafkaTemplate();
    }

    @Override
    public KafkaTemplate<K, V> getTemplate(final KafkaEmbedded embeddedKafka, final Optional<String> defaultTopic)
            throws Exception {

        return new TestingTemplate<K, V>(embeddedKafka, defaultTopic).getKafkaTemplate();
    }

    @Override
    public KafkaMessageListenerContainer<K, V> getContainer() throws Exception {
        return new TestingContainer<K, V>(defaultEmbeddedKafka).getContainer();
    }

    @Override
    public KafkaMessageListenerContainer<K, V> getContainer(final KafkaEmbedded embeddedKafka) throws Exception {
        return new TestingContainer<K, V>(embeddedKafka).getContainer();
    }

    @Override
    public KafkaMessageListenerContainer<K, V> getContainer(final String... topics) throws Exception {
        return new TestingContainer<K, V>(defaultEmbeddedKafka).getContainer();
    }

    @Override
    public KafkaMessageListenerContainer<K, V> getContainer(final KafkaEmbedded embeddedKafka, final String... topics)
            throws Exception {

        return new TestingContainer<K, V>(embeddedKafka, topics).getContainer();
    }

}
