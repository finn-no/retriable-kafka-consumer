package no.finn.retriableconsumer

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.Producer
import org.junit.Test
import org.mockito.Mockito.mock
import java.util.function.Function
import kotlin.test.assertEquals

@Suppress("UNCHECKED_CAST")
class ReliableKafkaConsumerPoolTest {
    private val NULL_LOG_HANDLER = NullLogHandler()

    @Test
    fun `Create consumerpool with size 3, 1 consumer, 1 producer and 0 retry-consumer`() {
        val factory = object : KafkaClientFactory<String, String> {
            override fun producer(): Producer<String, String> = mock(Producer::class.java) as Producer<String, String>
            override fun groupId() = "mockTopic"
            override fun consumer(): Consumer<String, String> = mock(Consumer::class.java) as Consumer<String, String>
        }

        val processingFunction = Function<ConsumerRecord<String, String>, Boolean> { throw NotImplementedError("Not in use") }
        val pollFunction = Function<Consumer<String, String>, ConsumerRecords<String, String>> { throw NotImplementedError("Not in use") }
        val pool = ReliableKafkaConsumerPool(1, factory, mapOf("topic" to "retry-topic"), processingFunction, pollFunction, java.util.function.Consumer {}, NULL_LOG_HANDLER, 100, 100_000)
        assertEquals(2, pool.size())
    }
}
