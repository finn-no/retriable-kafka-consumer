package no.finntech.retriableconsumer;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;


import io.prometheus.client.Counter;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static no.finntech.retriableconsumer.version.ExposeVersion.getApplicationNameForPrometheus;

public class RestartableKafkaConsumer<K, V> implements Restartable {

    private static final Counter EXPIRED_EVENTS_COUNTER =
            Counter.build()
                    .namespace(getApplicationNameForPrometheus())
                    .name("expired_events")
                    .labelNames("topic")
                    .help("Events expired on retry queue. Must be handled manually")
                    .register();

    private static final Counter FAILED_EVENTS_COUNTER =
            Counter.build()
                    .namespace(getApplicationNameForPrometheus())
                    .name("failed_events")
                    .labelNames("topic")
                    .help("Events failed.")
                    .register();

    private static final Counter PROCESSED_SUCCESSFULLY_EVENTS_COUNTER =
            Counter.build()
                    .namespace(getApplicationNameForPrometheus())
                    .name("processed_successfully_events")
                    .labelNames("topic")
                    .help("Events successfully processed.")
                    .register();


    public static final String HEADER_TIMESTAMP_KEY = "retryable-timestamp-sent";

    private static final Logger log = LoggerFactory.getLogger(RestartableKafkaConsumer.class);
    private static final AtomicInteger consumerCounter = new AtomicInteger(0);

    private final String consumerName;
    private final AtomicBoolean running = new AtomicBoolean();
    private final Function<Consumer<K, V>, ConsumerRecords<K, V>> pollFunction;
    private final Function<ConsumerRecord<K, V>, Boolean> processingFunction;
    private final java.util.function.Consumer<ConsumerRecord<K, V>> retryConsumer;
    private final List<String> topics;
    private final Function<Consumer<K, V>, Void> afterProcess;
    private final long retryDuration;
    private final Supplier<Consumer<K, V>> consumerFactory;


    RestartableKafkaConsumer(
            Supplier<Consumer<K, V>> consumerFactory,
            List<String> topics,
            Function<ConsumerRecord<K, V>, Boolean> processRecord,
            Function<Consumer<K, V>, ConsumerRecords<K, V>> pollFunction,
            Function<Consumer<K, V>, Void> afterProcess,
            java.util.function.Consumer<ConsumerRecord<K, V>> retryHandler,
            long retryDurationInMillis) {
        this.consumerFactory = consumerFactory;
        this.topics = topics;
        this.processingFunction = processRecord;
        this.pollFunction = kvConsumer -> {
            try {
                return pollFunction.apply(kvConsumer);
            } catch (RetriableException e) {
                log.warn("Got a RetriableException, continue", e);
                return ConsumerRecords.empty();
            }
        };
        this.afterProcess = afterProcess;
        this.retryConsumer = retryHandler;
        this.consumerName = "restartableConsumer-" + consumerCounter.getAndIncrement();
        for (String topic : topics) {
            EXPIRED_EVENTS_COUNTER.labels(topic).inc(0);
            FAILED_EVENTS_COUNTER.labels(topic).inc(0);
        }
        this.retryDuration = retryDurationInMillis;
    }

    @Override
    public void run() {
        log.info("Started consumer");
        running.set(true);
        try (Consumer<K, V> consumer = consumerFactory.get()) {
            ensureSubscribtionTo(topics, consumer);
            while (running.get()) {

                ConsumerRecords<K, V> record = pollFunction.apply(consumer);
                long start = System.currentTimeMillis();
                for (ConsumerRecord<K, V> kvConsumerRecord : record) {
                    if (processCount(kvConsumerRecord.headers()) > 0) {
                        log.info("Reprocess counter is {} for event {}", processCount(kvConsumerRecord.headers()), kvConsumerRecord.value());
                    }
                    if (isExpired(kvConsumerRecord, retryDuration)) {
                        log.warn("Event was expired and discarded {}.", kvConsumerRecord);
                        EXPIRED_EVENTS_COUNTER.labels(kvConsumerRecord.topic()).inc();
                        continue;
                    }
                    try {
                        if (!processingFunction.apply(kvConsumerRecord)) {
                            log.error("Processing returned failure, adding to failqueue.");
                            retryConsumer.accept(kvConsumerRecord);
                            FAILED_EVENTS_COUNTER.labels(kvConsumerRecord.topic()).inc();
                        }
                        PROCESSED_SUCCESSFULLY_EVENTS_COUNTER.labels(kvConsumerRecord.topic()).inc();
                    } catch (Exception failure) {
                        if (kvConsumerRecord.value() != null) {
                            log.error(
                                    "Processing threw exception when consuming from topic "
                                            + kvConsumerRecord.topic()
                                            + ". Adding message to failqueue: "
                                            + kvConsumerRecord.value(),
                                    failure);
                        } else {
                            log.error("Processing of null-value threw exception, ", failure);
                        }
                        retryConsumer.accept(kvConsumerRecord);
                        FAILED_EVENTS_COUNTER.labels(kvConsumerRecord.topic()).inc();
                    }
                }
                if (!record.isEmpty()) {
                    try {
                        afterProcess.apply(consumer);
                    } catch (CommitFailedException | RetriableException cfe) {
                        log.warn("Commit failed ", cfe);
                    }
                    log.debug("Spent {} millis from poll to commit", (System.currentTimeMillis() - start));
                }
            }
        } catch (Throwable e) {
            log.error("Consumer {} failed", getName(), e);
        } finally {
            this.close();
        }
        log.info("Consumer stopped");
    }

    static int processCount(Headers headers) {
        try {
            if (headers.lastHeader(RetryHandler.HEADER_KEY_REPROCESS_COUNTER) != null) {
                return Integer.parseInt(new String(headers.lastHeader(RetryHandler.HEADER_KEY_REPROCESS_COUNTER).value()));
            }
        } catch (Exception e) {
            log.warn("Invalid reprocess-counter in header", e);
        }
        return 0;
    }

    static boolean isExpired(ConsumerRecord<?, ?> kvConsumerRecord, long retryDuration) {
        if (kvConsumerRecord == null || kvConsumerRecord.headers().lastHeader(HEADER_TIMESTAMP_KEY) == null) {
            // no timestamp in header, not able to determine expiration
            return false;
        }

        Header timestampHeader = kvConsumerRecord.headers().lastHeader(HEADER_TIMESTAMP_KEY);

        String timestampString = new String(timestampHeader.value());

        if (!NumberUtils.isDigits(timestampString)) {
            log.warn("Timestamp is corrupt, expected digits only, got {}", timestampString);
            return false;
        }

        long timestamp = Long.parseLong(timestampString);

        return (System.currentTimeMillis() - timestamp) > retryDuration;
    }

    private void ensureSubscribtionTo(List<String> topics, Consumer<K, V> consumer) {
        Optional<Boolean> notAssigned = topics.stream().map(s -> isAssignedToTopic(consumer, s)).findAny().filter(p -> !p);
        if (notAssigned.isPresent()) {
            consumer.subscribe(topics);
        }
    }

    private boolean isAssignedToTopic(Consumer<K, V> consumer, String topic) {
        try {
            return consumer.assignment().stream().anyMatch(a -> a.topic().equalsIgnoreCase(topic));
        } catch (Exception e) {
            return false;
        }
    }


    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public String getName() {
        return consumerName;
    }


    @Override
    public void close() {
        log.info("Closing consumer " + getName());
        running.set(false);
    }
}
