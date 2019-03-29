package no.finntech.retriableconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestartableRetryProducerTest {

    @Test
    public void topic_name() {

        String retryTopic = RetryHandler.retryTopicName("sometopic", "mygroup");

        assertThat(retryTopic).isEqualToIgnoringCase("retry-mygroup-sometopic");
    }

    @Test
    public void dont_prefix_if_already_prefixed() {

        String retryTopic = RetryHandler.retryTopicName("retry-sometopic", "mygroup");

        assertThat(retryTopic).isEqualToIgnoringCase("retry-sometopic");
    }

    @Test
    public void create_producer_record_with_correct_headers_for_first_time_failing_Record() {

        RetryHandler<String, String> producer = new RetryHandler<>( ()->null, 500, "");

        ConsumerRecord<String, String> oldRecord = new ConsumerRecord<>("topic", 0, 0, "key", "value");
        ProducerRecord retryRecord = producer.createRetryRecord(oldRecord, "retry-topic", 1000);


        List<Header> retryHeaders = Arrays.asList(retryRecord.headers().toArray());
        assertThat(retryHeaders.size()).isEqualTo(2);
        assertThat(retryHeaders.get(0).key()).isEqualTo(RetryHandler.HEADER_KEY_REPROCESS_COUNTER);
        assertThat(retryHeaders.get(0).value()).isEqualTo("1".getBytes());
        assertThat(retryHeaders.get(1).key()).isEqualTo(RestartableKafkaConsumer.HEADER_TIMESTAMP_KEY);
        assertThat(retryHeaders.get(1).value()).isEqualTo("1000".getBytes());

    }

    @Test
    public void conserve_timestamp_header_increse_counter_header() {

        RetryHandler<String, String> producer = new RetryHandler<>( ()->null, 500, "");

        ConsumerRecord<String, String> oldRecord = new ConsumerRecord<>("topic", 0, 0, "key", "value");
        oldRecord.headers().add(new RecordHeader(RetryHandler.HEADER_KEY_REPROCESS_COUNTER, "41".getBytes()));
        oldRecord.headers().add(new RecordHeader(RestartableKafkaConsumer.HEADER_TIMESTAMP_KEY, "1337".getBytes()));

        ProducerRecord retryRecord = producer.createRetryRecord(oldRecord, "retry-topic", 1000);


        List<Header> retryHeaders = Arrays.asList(retryRecord.headers().toArray());
        assertThat(retryHeaders.size()).isEqualTo(2);

        assertThat(retryHeaders.get(1).key()).isEqualTo(RetryHandler.HEADER_KEY_REPROCESS_COUNTER);
        assertThat(new String(retryHeaders.get(1).value())).isEqualTo("42");
        assertThat(retryHeaders.get(0).key()).isEqualTo(RestartableKafkaConsumer.HEADER_TIMESTAMP_KEY);
        assertThat(new String(retryHeaders.get(0).value())).isEqualTo("1337");

    }


    @Test
    public void parse_produced_counter_headers() {

        RetryHandler<String, String> producer = new RetryHandler<>( ()->null, 500, "");

        ConsumerRecord<String, String> oldRecord = new ConsumerRecord<>("topic", 0, 0, "key", "value");
        ProducerRecord retryRecord = producer.createRetryRecord(oldRecord, "retry-topic", 1000);


        List<Header> retryHeaders = Arrays.asList(retryRecord.headers().toArray());
        assertThat(retryHeaders.size()).isEqualTo(2);
        assertThat(retryHeaders.get(0).key()).isEqualTo(RetryHandler.HEADER_KEY_REPROCESS_COUNTER);

        assertThat(RestartableKafkaConsumer.processCount(retryRecord.headers())).isEqualTo(1);

    }


    @Test
    public void parse_produced_timestamp_header() {

        RetryHandler<String, String> producer = new RetryHandler<>( ()->null, 500, "");

        ConsumerRecord<String, String> oldRecord = new ConsumerRecord<>("topic", 0, 0, "key", "value");
        ProducerRecord retryRecord = producer.createRetryRecord(oldRecord, "retry-topic", 1000);


        List<Header> retryHeaders = Arrays.asList(retryRecord.headers().toArray());
        assertThat(retryHeaders.size()).isEqualTo(2);
        assertThat(retryHeaders.get(1).key()).isEqualTo(RestartableKafkaConsumer.HEADER_TIMESTAMP_KEY);

        String timestampString = new String(retryHeaders.get(1).value());

        assertThat(timestampString).isEqualTo("1000");

    }


    @Test
    public void send_to_kafka_when_record_available_on_queue() throws Exception {

        Producer<String, String> kafkaProducerMock = mock(Producer.class);
        when(kafkaProducerMock.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture("jadda"));
        ArgumentCaptor<ProducerRecord> kafkaArgumentCaptor =
                ArgumentCaptor.forClass(ProducerRecord.class);

        ConsumerRecord<String, String> failedRecord = new ConsumerRecord<>("foo", 0, 0, "bar", "baz");

        RetryHandler<String, String> retryProducer =new RetryHandler<>(()->kafkaProducerMock,100, "groupid");

        retryProducer.accept(failedRecord);

        verify(kafkaProducerMock).send(kafkaArgumentCaptor.capture());

        ProducerRecord sentRecord = kafkaArgumentCaptor.getValue();

        assertThat(sentRecord.topic()).isEqualToIgnoringCase("retry-groupid-foo");
        assertThat(sentRecord.key().toString()).isEqualToIgnoringCase("bar");
        assertThat(sentRecord.value().toString()).isEqualToIgnoringCase("baz");
    }

    @Test
    public void reprocess_header() {

        Header header = RetryHandler.processCounterHeader(new ProducerRecord<>("", "", ""));

        assertThat(header.key()).isEqualTo(RetryHandler.HEADER_KEY_REPROCESS_COUNTER);


        ProducerRecord<String, String> secondRecord = new ProducerRecord<>("", "", "");
        secondRecord.headers().add(header);

        Header header_second_process = RetryHandler.processCounterHeader(secondRecord);


        assertThat(header_second_process.value()).isEqualTo("2".getBytes());

    }
}
