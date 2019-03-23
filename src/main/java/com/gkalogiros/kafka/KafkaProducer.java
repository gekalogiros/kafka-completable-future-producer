package com.gkalogiros.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class KafkaProducer<K,V> extends org.apache.kafka.clients.producer.KafkaProducer<K, V> {

    public KafkaProducer(Map<String, Object> configs) {
        super(configs);
    }

    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(configs, keySerializer, valueSerializer);
    }

    public KafkaProducer(Properties properties) {
        super(properties);
    }

    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(properties, keySerializer, valueSerializer);
    }

    public CompletableFuture<RecordMetadata> send(ProducerRecord<K, V> record) {
        CompletableFuture<RecordMetadata> promise = new CompletableFuture<>();
        send(record, (recordMetadata, e) -> {
            Optional.ofNullable(recordMetadata).ifPresent(promise::complete);
            Optional.ofNullable(e).ifPresent(promise::completeExceptionally);
        });
        return promise;
    }
}
