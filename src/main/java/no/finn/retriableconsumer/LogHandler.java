package no.finn.retriableconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface LogHandler<K, V> {
    void logExpired(ConsumerRecord<K, V> record, int numRetries);

    void logRetry(ConsumerRecord<K, V> record, int numRetries, String retryTopic);

    void logException(ConsumerRecord<K, V> record, Exception e);
}
