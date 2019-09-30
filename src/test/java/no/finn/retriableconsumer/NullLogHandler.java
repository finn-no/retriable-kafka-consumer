package no.finn.retriableconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class NullLogHandler implements LogHandler<String, String> {
    @Override
    public void logExpired(ConsumerRecord<String, String> record, int numRetries) {
    }

    @Override
    public void logRetry(ConsumerRecord<String, String> record, int numRetries, String retryTopic) {
    }

    @Override
    public void logException(ConsumerRecord<String, String> record, Exception e) {
    }
}
