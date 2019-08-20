package no.finn.retriableconsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Function;

import no.finn.retriableconsumer.version.ExposeVersion;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class ReliablePoolBuilder<K, V> {
    static {
        // register version to prometheus
        ExposeVersion.init();
    }

    private Function<Consumer<K, V>, ConsumerRecords<K, V>> pollFunction = consumer -> consumer.poll(Duration.of(250, ChronoUnit.MILLIS));
    private Integer poolCount = 3;
    private final KafkaClientFactory<K, V> factory;
    private List<String> topics;
    private Function<ConsumerRecord<K, V>, Boolean> processingFunction;
    private Long retryThrottleMillis = 5000L;
    private Long retryPeriodMillis = 24 * 60 * 60 * 1000L; // 1 day by default

    public ReliablePoolBuilder(KafkaClientFactory<K, V> factory) {
        this.factory = factory;
    }

    public ReliablePoolBuilder<K, V> pollFunction(Function<Consumer<K, V>, ConsumerRecords<K, V>> poolFunction) {
        this.pollFunction = poolFunction;
        return this;
    }

    public ReliablePoolBuilder<K, V> poolCount(int poolCount) {
        this.poolCount = poolCount;
        return this;
    }

    public ReliablePoolBuilder<K, V> topics(List<String> topics) {
        this.topics = topics;
        return this;
    }

    public ReliablePoolBuilder<K, V> processingFunction(Function<ConsumerRecord<K, V>, Boolean> processingFunction) {
        this.processingFunction = processingFunction;
        return this;
    }

    /**
     * Millis to wait before a failed message is retried - defaults to 5 secounds (5_000 milliseconds)
     *
     * @param retryThrottleMillis millis to sleep before record is retried
     * @return the builder
     */
    public ReliablePoolBuilder<K, V> retryThrottleMillis(long retryThrottleMillis) {
        this.retryThrottleMillis = retryThrottleMillis;
        return this;
    }

    /**
     * Total millis the message is allowed to live before it is discarded - defaults to 1 hour
     *
     * @param retryPeriodMillis number of millis a record is allowed to live before it is discarded
     * @return the builder
     */
    public ReliablePoolBuilder<K, V> retryPeriodMillis(long retryPeriodMillis) {
        this.retryPeriodMillis = retryPeriodMillis;
        return this;
    }


    public ReliableKafkaConsumerPool<K, V> build() {
        verifyNotNull("pollFunction", pollFunction);
        verifyNotNull("poolCount", poolCount);
        verifyNotNull("topics", topics);
        verifyNotNull("processingFunction", processingFunction);
        verifyNotNull("retryThrottleMillis", retryThrottleMillis);
        verifyNotNull("retryPeriodMillis", retryPeriodMillis);
        verifyNotNull("factory", factory);

        return new ReliableKafkaConsumerPool<>(poolCount, factory, topics, processingFunction, pollFunction, retryThrottleMillis, retryPeriodMillis);
    }

    private void verifyNotNull(String fieldName, Object field) {
        if (field == null) {
            throw new IllegalStateException(fieldName + " is not set, cannot build an instance of" + ReliableKafkaConsumerPool.class.getCanonicalName());
        }
    }
}
