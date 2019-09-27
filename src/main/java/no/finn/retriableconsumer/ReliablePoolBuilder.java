package no.finn.retriableconsumer;

import no.finn.retriableconsumer.version.ExposeVersion;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ReliablePoolBuilder<K, V> {
    static {
        // register version to prometheus
        ExposeVersion.init();
    }

    private Function<Consumer<K, V>, ConsumerRecords<K, V>> pollFunction = consumer -> consumer.poll(Duration.of(250, ChronoUnit.MILLIS));
    private Integer poolCount = 3;
    private final KafkaClientFactory<K, V> factory;
    private Function<ConsumerRecord<K, V>, Boolean> processingFunction;
    private Long retryThrottleMillis = 5000L;
    private Long retryPeriodMillis = 24 * 60 * 60 * 1000L; // 1 day by default
    private Map<String, String> topicsRetryTopics;
    private java.util.function.Consumer<ConsumerRecord<K, V>> expiredHandler = kvConsumerRecord -> {
    };

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
        return topicsRetryTopics(topics.stream().collect(Collectors.toMap(topic -> topic, topic -> retryTopicName(topic, factory.groupId()))));
    }

    public ReliablePoolBuilder<K, V> topicsRetryTopics(Map<String, String> topicsRetryTopics) {
        HashMap<String, String> topicsWithRetryMapping = new HashMap<>();
        topicsWithRetryMapping.putAll(topicsRetryTopics);
        topicsWithRetryMapping.putAll(topicsRetryTopics.values().stream().collect(Collectors.toMap(retryTopic -> retryTopic, retryTopic -> retryTopic)));
        this.topicsRetryTopics = topicsWithRetryMapping;
        return this;
    }

    public ReliablePoolBuilder<K, V> processingFunction(Function<ConsumerRecord<K, V>, Boolean> processingFunction) {
        this.processingFunction = processingFunction;
        return this;
    }


    public ReliablePoolBuilder<K, V> expiredHandler(java.util.function.Consumer<ConsumerRecord<K, V>> expiredHandler) {
        this.expiredHandler = expiredHandler;
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
        verifyNotNull("topicsRetryTopics", topicsRetryTopics);
        verifyNotNull("processingFunction", processingFunction);
        verifyNotNull("expiredHandler", expiredHandler);
        verifyNotNull("retryThrottleMillis", retryThrottleMillis);
        verifyNotNull("retryPeriodMillis", retryPeriodMillis);
        verifyNotNull("factory", factory);

        return new ReliableKafkaConsumerPool<>(poolCount, factory, topicsRetryTopics, processingFunction, expiredHandler, pollFunction, retryThrottleMillis, retryPeriodMillis);
    }

    private void verifyNotNull(String fieldName, Object field) {
        if (field == null) {
            throw new IllegalStateException(fieldName + " is not set, cannot build an instance of" + ReliableKafkaConsumerPool.class.getCanonicalName());
        }
    }

    static String retryTopicName(String topic, String groupId) {
        if (StringUtils.startsWith(topic, "retry")) return topic;
        return String.format("%s-%s-%s", "retry", groupId, topic);
    }
}
