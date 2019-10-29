package no.finn.retriableconsumer;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryHandler<K, V> implements Consumer<ConsumerRecord<K, V>> {

    @SuppressWarnings("WeakerAccess")
    public static String HEADER_KEY_REPROCESS_COUNTER = "reprocess-counter";

    private static final Logger log = LoggerFactory.getLogger(RetryHandler.class);

    private final Supplier<Producer<K, V>> factory;
    private final long retryThrottleMillis;
    private final Map<String, String> topicsRetryTopics;
    private final LogHandler<K, V> logHandler;

    RetryHandler(Supplier<Producer<K, V>> factory,
                 long retryThrottleMillis,
                 Map<String, String> topicsRetryTopics,
                 LogHandler<K, V> logHandler) {
        this.factory = factory;
        this.retryThrottleMillis = retryThrottleMillis;
        this.topicsRetryTopics = topicsRetryTopics;
        this.logHandler = logHandler;
    }


    @Override
    public void accept(ConsumerRecord<K, V> record) {
        String retryTopic = topicsRetryTopics.get(record.topic());
        ProducerRecord<K, V> retryRecord = createRetryRecord(record, retryTopic, System.currentTimeMillis());
        int counter = Integer.parseInt(new String(retryRecord.headers().lastHeader(HEADER_KEY_REPROCESS_COUNTER).value()));
        logHandler.logRetry(record, counter, retryTopic);
        try {
            factory.get().send(retryRecord).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed waiting for retry message sent confirmation", e);
        }
        try {
            Thread.sleep(retryThrottleMillis);
        } catch (InterruptedException e) {
            log.error("Interrupted while sleeping", e);
        }
    }

    ProducerRecord<K, V> createRetryRecord(ConsumerRecord<K, V> oldRecord, String retryTopic, long nowInMillis) {
        ProducerRecord<K, V> newRecord = new ProducerRecord<>(retryTopic, oldRecord.key(), oldRecord.value());

        // copy headers from consumer
        oldRecord.headers().forEach(h -> newRecord.headers().add(h));

        // add reprocessCounter header
        Header counterHeader = processCounterHeader(newRecord);
        newRecord.headers().remove(HEADER_KEY_REPROCESS_COUNTER);
        newRecord.headers().add(counterHeader);

        // add timestamp-header if not present
        if (newRecord.headers().lastHeader(RestartableKafkaConsumer.HEADER_TIMESTAMP_KEY) == null) {
            newRecord.headers().add(timestampHeader(nowInMillis));
        }
        return newRecord;
    }

    public static Header timestampHeader(long timestamp) {
        return new RecordHeader(RestartableKafkaConsumer.HEADER_TIMESTAMP_KEY, String.valueOf(timestamp).getBytes());
    }

    static Header processCounterHeader(ProducerRecord<?, ?> producerRecord) {
        Header processCounterHeader = producerRecord.headers().lastHeader(HEADER_KEY_REPROCESS_COUNTER);
        if (processCounterHeader == null || !NumberUtils.isDigits(new String(processCounterHeader.value()))) {
            return new RecordHeader(HEADER_KEY_REPROCESS_COUNTER, "1".getBytes());
        }

        int reprocessCount = Integer.parseInt(new String(processCounterHeader.value()));
        return new RecordHeader(HEADER_KEY_REPROCESS_COUNTER, String.valueOf(reprocessCount + 1).getBytes());
    }
}
