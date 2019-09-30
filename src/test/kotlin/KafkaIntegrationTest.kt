import no.finn.retriableconsumer.KafkaClientFactory
import no.finn.retriableconsumer.NullLogHandler
import no.finn.retriableconsumer.ReliableKafkaConsumerPool
import no.finn.retriableconsumer.RetryHandler
import no.finn.retriableconsumer.TestUtil.assertWithinTimespan
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.TopicPartition
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function


@Suppress("UNCHECKED_CAST", "DEPRECATION")
class KafkaIntegrationTest {
    @Test
    fun `Dont process records that has timed out`() {
        val processCounter = AtomicInteger(0)

        val consumer = Mockito.mock(Consumer::class.java) as Consumer<String, String>
        Mockito.`when`(consumer.poll(anyLong())).thenReturn(consumerRecordsWithTimeout()).thenReturn(null)

        val factory = object : KafkaClientFactory<String, String> {
            override fun producer(): Producer<String, String> = Mockito.mock(Producer::class.java) as Producer<String, String>
            override fun groupId() = "mockTopic"
            override fun consumer(): Consumer<String, String> {
                return consumer
            }
        }

        val process = Function<ConsumerRecord<String, String>, Boolean> {
            processCounter.incrementAndGet()
            true
        }
        val pollFunction = Function<Consumer<String, String>, ConsumerRecords<String, String>> { it.poll(1) }

        val pool = ReliableKafkaConsumerPool(3, factory, mapOf("foo" to "retry-foo"), process, pollFunction, java.util.function.Consumer {}, NULL_LOG_HANDLER, 10, 10_000)

        pool.monitor.start()

        assertWithinTimespan({ assertThat(processCounter.get()).isEqualTo(1) }, 5000L)

        pool.monitor.close()
    }


    @Test
    fun `Reprocess failed events`() {
        val processCounter = AtomicInteger(0)

        val consumer = Mockito.mock(Consumer::class.java) as Consumer<String, String>
        Mockito.`when`(consumer.poll(anyLong()))
                .thenReturn(consumerRecordsWithTimeout())
                .thenReturn(ConsumerRecords(hashMapOf()))

        val producer = Mockito.mock(Producer::class.java) as Producer<String, String>

        val factory = object : KafkaClientFactory<String, String> {
            override fun producer(): Producer<String, String> {
                return producer
            }

            override fun groupId() = "mockTopic"
            override fun consumer(): Consumer<String, String> {
                return consumer
            }
        }

        val process = Function<ConsumerRecord<String, String>, Boolean> {
            if (processCounter.incrementAndGet() == 1) throw RuntimeException("BANG!")// fail first time
            true
        }
        val poll = Function<Consumer<String, String>, ConsumerRecords<String, String>> { it.poll(1) }

        val pool = ReliableKafkaConsumerPool(3, factory, mapOf("foo" to "retry-foo"), process, poll, java.util.function.Consumer {}, NULL_LOG_HANDLER, 10, 10_000_000)

        pool.monitor.start()

        assertWithinTimespan({ assertThat(processCounter.get()).isEqualTo(1) }, 5000L)
        assertWithinTimespan({ verify(producer, times(1)).send(any()) }, 5000L)

        pool.monitor.close()
    }


    private fun consumerRecordsWithTimeout(): ConsumerRecords<String, String>? {
        val mape = HashMap<TopicPartition, MutableList<ConsumerRecord<String, String>>>()
        val recordWithoutTimeout = ConsumerRecord<String, String>("foo", 0, 0L, "key", "value")

        val recordWithTimeoutZero = ConsumerRecord<String, String>("foo", 0, 0L, "key", "value")

        recordWithTimeoutZero.headers().add(RetryHandler.timestampHeader(0))

        mape[TopicPartition("foo", 1)] = ArrayList(listOf(recordWithTimeoutZero, recordWithoutTimeout))
        return ConsumerRecords(mape)
    }


    companion object {

        private val NULL_LOG_HANDLER = NullLogHandler()

    }
}
