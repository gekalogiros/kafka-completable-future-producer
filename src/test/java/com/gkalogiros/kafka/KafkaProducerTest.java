package com.gkalogiros.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaProducerTest {

    @ClassRule
    public static KafkaTestingCluster inMemoryKafka = new KafkaTestingCluster();

    private KafkaProducer<String, String> underTest;

    @Before
    public void setUp() {

        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, inMemoryKafka.getKafkaServers());

        this.underTest = new KafkaProducer<>(properties);
    }

    @Test
    public void send() {

        final String key = "key";
        final String value = "value";
        final String topic = "test-topic";

        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
        final CompletableFuture<RecordMetadata> completableFuture = underTest.send(producerRecord);

        assertThat(completableFuture).isCompletedWithValueMatching(recordMetadata -> {
            assertThat(recordMetadata.topic()).isEqualTo(topic);
            return true;
        });
    }
}