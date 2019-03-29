package no.finntech.retriableconsumer;

import com.google.common.collect.Lists;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class IntegrationtestKafkaClientFactory<T> implements KafkaClientFactory<String, T> {
    private final SharedKafkaTestResource sharedKafkaTestResource;
    private final Class<? extends Serializer<T>> valueSerializer;
    private final Class<? extends Deserializer<T>> valueDeserializer;
    private final List<String> topics;

    private Producer<String, T> producer;

    private AtomicInteger consumerCounter = new AtomicInteger();

    public IntegrationtestKafkaClientFactory(
            SharedKafkaTestResource sharedKafkaTestResource,
            List<String> topics,
            Class<? extends Serializer<T>> valueSerializer,
            Class<? extends Deserializer<T>> valueDeserializer) {
        assert sharedKafkaTestResource != null : "SharedKafkaTestResource cannot be null";
        assert valueSerializer != null : "serializer cannot be null";
        assert valueDeserializer != null : "deserializer cannot be null";
        assert !topics.isEmpty() : "topic cannot be blank";
        this.sharedKafkaTestResource = sharedKafkaTestResource;
        this.valueSerializer = valueSerializer;
        this.valueDeserializer = valueDeserializer;
        this.topics = topics;
        for (String topic : topics) {
            sharedKafkaTestResource.getKafkaTestUtils().createTopic(topic, 1, Short.valueOf("1"));
        }
    }

    @Override
    public Consumer<String, T> consumer() {
        int consumerNo = consumerCounter.getAndIncrement();
        Consumer<String, T> consumer =
                sharedKafkaTestResource
                        .getKafkaTestUtils()
                        .getKafkaConsumer(StringDeserializer.class, valueDeserializer);
        final List<TopicPartition> topicPartitionList = Lists.newArrayList();
        for (String topic : topics) {
            for (final PartitionInfo partitionInfo : consumer.partitionsFor(topic)) {
                if (partitionInfo.partition() == consumerNo) {
                    topicPartitionList.add(
                            new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                }
            }
        }
        consumer.assign(topicPartitionList);
        consumer.seekToBeginning(topicPartitionList);
        return consumer;
    }

    @Override
    public Producer<String, T> producer() {
        if (producer == null) {
            producer =
                    sharedKafkaTestResource
                            .getKafkaTestUtils()
                            .getKafkaProducer(StringSerializer.class, valueSerializer);
        }
        return producer;
    }

    @Override
    public String groupId() {
        return null;
    }
}
