package no.finn.retriableconsumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestartableKafkaConsumerTest {
    private Consumer<String, String> mockConsumer;

    private static final List<String> TOPIC = Arrays.asList("topic", "topic1");
    private List<ConsumerRecord<String, String>> consumerRecords =
            Arrays.asList(
                    new ConsumerRecord<>(TOPIC.get(0), 0, 1, "key1", "val1"),
                    new ConsumerRecord<>(TOPIC.get(0), 0, 1, "key2", "val2"));

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        mockConsumer = Mockito.mock(Consumer.class);

        when(mockConsumer.poll(anyLong()))
                .thenReturn(
                        new ConsumerRecords<>(
                                new HashMap<TopicPartition, List<ConsumerRecord<String, String>>>() {
                                    {
                                        put(new TopicPartition(TOPIC.get(0), 0), consumerRecords);
                                    }
                                }))
                .thenThrow(new RuntimeException("Fail thread on 2nd invocation to poll"));
    }

    @Test
    public void expired() {
        Header timestampHeaderOneMinuteAgo = new RecordHeader(RestartableKafkaConsumer.HEADER_TIMESTAMP_KEY, String.valueOf(System.currentTimeMillis() - 60 * 1000).getBytes());

        assertThat(RestartableKafkaConsumer.isExpired(new ConsumerRecord<>("", 0, 0, "", ""), 0)).isFalse()
                .describedAs("Not expired if timestamp header is not present");

        assertThat(RestartableKafkaConsumer.isExpired(
                new ConsumerRecord<>("",
                        0,
                        0,
                        0,
                        TimestampType.NO_TIMESTAMP_TYPE,
                        0L,
                        0,
                        0,
                        "",
                        "",
                        new RecordHeaders(Arrays.asList(timestampHeaderOneMinuteAgo))),
                100_000_000))
                .isFalse()
                .describedAs("Should not be expired if timestamp header is present and expirytime is not passed");

        assertThat(RestartableKafkaConsumer.isExpired(
                new ConsumerRecord<>("",
                        0,
                        0,
                        0,
                        TimestampType.NO_TIMESTAMP_TYPE,
                        0L,
                        0,
                        0,
                        "",
                        "",
                        new RecordHeaders(Arrays.asList(timestampHeaderOneMinuteAgo))),
                0))
                .isTrue()
                .describedAs("Should be expired if timestamp header is present and expirytime is passed");

    }

    @Test
    public void process_until_exception_arises_and_put_on_fail_queue() {


        AtomicInteger processCount = new AtomicInteger(0);

        Function<ConsumerRecord<String, String>, Boolean> processFunction =
                s -> {
                    if (processCount.incrementAndGet() > 1) {
                        throw new RuntimeException("Fail on 2nd processing");
                    }
                    return Boolean.TRUE;
                };

        java.util.function.Consumer<ConsumerRecord<String, String>> failedQueue = mock(java.util.function.Consumer.class);
        RestartableKafkaConsumer<String, String> consumer =
                new RestartableKafkaConsumer<>(
                        () -> mockConsumer, TOPIC, processFunction, s -> s.poll(10), failedQueue, 100_000_000);

        consumer.run();

        assertThat(consumer.isRunning()).isFalse();
        assertThat(processCount.get()).isEqualTo(2);

        ArgumentCaptor<ConsumerRecord> captor = ArgumentCaptor.forClass(ConsumerRecord.class);
        verify(failedQueue).accept(captor.capture());
        assertThat(captor.getAllValues()).contains(consumerRecords.get(1));
    }

    @Test
    public void put_on_fail_queue_if_process_returns_false() {


        AtomicInteger processCount = new AtomicInteger(0);

        Function<ConsumerRecord<String, String>, Boolean> processFunction = s -> Boolean.FALSE;


        Producer<String, String> mockProducer = mock(Producer.class);
        Map<String, String> topicsRetryTopic = new HashMap<String, String>() {{
            put("topic", "retry-topic");
            put("topic1", "retry-topic1");
        }};
        RetryHandler<String, String> failer = new RetryHandler<>(() -> mockProducer, 100, topicsRetryTopic);


        RestartableKafkaConsumer<String, String> consumer =
                new RestartableKafkaConsumer<>(
                        () -> mockConsumer, TOPIC, processFunction, s -> s.poll(1000), failer, 100_000_000);

        consumer.run();

        assertThat(consumer.isRunning()).isFalse();
        assertThat(processCount.get()).isEqualTo(0);

        ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer, times(consumerRecords.size())).send(captor.capture());
        assertThat(captor.getAllValues()).hasSize(consumerRecords.size());
    }

    @Test
    public void dont_process_if_expired() {

        java.util.function.Consumer<ConsumerRecord<String, String>> failedQueue = mock(java.util.function.Consumer.class);

        Function<ConsumerRecord<String, String>, Boolean> processFunction =
                s -> {
                    throw new RuntimeException("Fail on each processing");
                };

        RestartableKafkaConsumer<String, String> consumer =
                new RestartableKafkaConsumer<>(
                        () -> mockConsumer, TOPIC, processFunction, s -> s.poll(10), failedQueue, 100_000_000);

        consumer.run();

        assertThat(consumer.isRunning())
                .isFalse()
                .describedAs("Should be stopped since 2nd poll fails");

        verify(failedQueue, times(2)).accept(any(ConsumerRecord.class));
    }
}
