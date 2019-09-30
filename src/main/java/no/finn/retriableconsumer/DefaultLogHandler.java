package no.finn.retriableconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultLogHandler<K, V> implements LogHandler<K, V> {
    private static final Logger log = LoggerFactory.getLogger(DefaultLogHandler.class);

    @Override
    public void logExpired(ConsumerRecord<K, V> record, int numRetries) {
        log.warn("Event was expired and discarded {}. Retried a total of {} times. ", record, numRetries);
    }

    @Override
    public void logRetry(ConsumerRecord<K, V> record, int numRetries, String retryTopic) {
        log.info("Putting message with key [{}] on retry-topic [{}]. num retries: {}", record.key(), retryTopic, numRetries);
    }

    @Override
    public void logException(ConsumerRecord<K, V> record, Exception e) {
        log.warn("Processing failed - will retry. topic: " + record.topic() + ", value: " + record.value(), e);
    }
}
