package no.finn.retriableconsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

public interface KafkaClientFactory<K, V> {

  Consumer<K, V> consumer();

  Producer<K, V> producer();

  String groupId();
}
