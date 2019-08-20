package no.finn.retriableconsumer

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.Producer
import org.junit.Test
import org.mockito.Mockito.mock
import java.util.*
import java.util.function.Function
import kotlin.test.assertEquals

class ReliableKafkaConsumerPoolTest {

    @Test
    fun `Create consumerpool with size 3, 1 consumer, 1 producer and 0 retry-consumer`() {
        val factory = object : KafkaClientFactory<String, String> {
            override fun producer(): Producer<String, String> = mock(Producer::class.java) as Producer<String, String>
            override fun groupId() = "mockTopic"
            override fun consumer(): Consumer<String, String> = mock(Consumer::class.java) as Consumer<String, String>
        }

        val processingFunction = Function<ConsumerRecord<String, String>, Boolean> { TODO("not in use") }
        val pollFunction = Function<Consumer<String, String>, ConsumerRecords<String, String>> { TODO("not in use") }

        val pool = ReliableKafkaConsumerPool(1, factory, Collections.singletonList("topic"), processingFunction, pollFunction,100, 100_000)

        assertEquals(2, pool.size())
    }
}
