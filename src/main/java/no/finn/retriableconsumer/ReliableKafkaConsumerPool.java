package no.finn.retriableconsumer;

import java.io.Closeable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import static java.util.stream.IntStream.rangeClosed;

/**
 * THE class to use from this library.
 *
 * <p>This class will create a set of consumers that subscribes to a given topic and mechanism to
 * resend records that fails to be processed.
 *
 * @param <K>  - the key-type of the kafka topic
 * @param <V>  - the value-type of the kafka-topic
 */
public class ReliableKafkaConsumerPool<K, V> implements Closeable {


    public final RestartableMonitor monitor;

    /**
     * @param consumerPoolCount   - number of kafka-consumers
     * @param factory             - factory that creates consumer and producer
     * @param topics               - topic to listen to
     * @param processingFunction  - function that will process messages
     * @param pollFunction        - function to poll messages
     * @param retryDurationInMillis - how long retry-producer should retry the message before giving up
     * @param retryThrottleMillis - how long we should delay before processing the record again
     */

    public ReliableKafkaConsumerPool(
            int consumerPoolCount,
            KafkaClientFactory<K, V> factory,
            List<String> topics,
            Function<ConsumerRecord<K, V>, Boolean> processingFunction,
            Function<Consumer<K, V>, ConsumerRecords<K, V>> pollFunction,
            long retryThrottleMillis,
            long retryDurationInMillis
    ) {

        // queue for safe communication between consumers and retry-producer
        RetryHandler<K, V> retryHandler = new RetryHandler<>(factory::producer, retryThrottleMillis, factory.groupId());

        // consumers
        List<Restartable> consumers =
                rangeClosed(1, consumerPoolCount)
                        .mapToObj(
                                i ->
                                        new RestartableKafkaConsumer<>(
                                                factory::consumer,
                                                topics,
                                                processingFunction,
                                                pollFunction,
                                                retryHandler,
                                                retryDurationInMillis))
                        .collect(Collectors.toList());

        //retry-consumer
        consumers.add(
                new RestartableKafkaConsumer<>(
                        factory::consumer,
                        RetryHandler.retryTopicNames(topics, factory.groupId()),
                        processingFunction,
                        pollFunction,
                        retryHandler,
                        retryDurationInMillis));

        // monitor all consumers and producer with the same monitor
        monitor = new RestartableMonitor(consumers);
    }

    @Override
    public void close() {
        monitor.close();
    }

    public int size() {
        return monitor.size();
    }
}
