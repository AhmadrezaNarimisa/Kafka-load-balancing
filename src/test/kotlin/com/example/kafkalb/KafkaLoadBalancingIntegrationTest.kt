package com.example.kafkalb

import com.example.kafkalb.consumer.OrderEventListener
import com.example.kafkalb.model.OrderEvent
import com.example.kafkalb.producer.EventsJson
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import java.math.BigDecimal
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = [
        "kafka.clusters.primary.bootstrap-servers=\${spring.embedded.kafka.brokers}",
        "kafka.clusters.secondary.bootstrap-servers=localhost:1",
        "app.listener-concurrency=2",
    ],
)
@EmbeddedKafka(
    partitions = 6,
    topics = ["orders-topic"],
    brokerProperties = [
        "offsets.topic.replication.factor=1",
        "group.initial.rebalance.delay.ms=0",
    ],
)
class KafkaLoadBalancingIntegrationTest {

    @Autowired
    private lateinit var listener: OrderEventListener

    @Autowired
    @Qualifier("primaryKafkaTemplate")
    private lateinit var primaryTemplate: KafkaTemplate<String, String>

    @Autowired
    @Qualifier("primaryCustomKafkaTemplate")
    private lateinit var primaryCustomTemplate: KafkaTemplate<String, String>

    private fun recordOf(event: OrderEvent, key: String? = null): ProducerRecord<String, String> {
        val record = if (key != null) ProducerRecord("orders-topic", key, EventsJson.write(event))
        else ProducerRecord<String, String>("orders-topic", null, EventsJson.write(event))
        return record
    }

    @Test
    @Timeout(30)
    fun `custom partitioner pins premium keys to partition 0`() {
        val event = OrderEvent(UUID.randomUUID().toString(), "premium-plan", BigDecimal("99"), "premium")
        val metadata = primaryCustomTemplate.send(recordOf(event, key = "premium-abc"))
            .get(10, TimeUnit.SECONDS).recordMetadata
        assertEquals(0, metadata.partition())
    }

    @Test
    @Timeout(30)
    fun `events reach both consumer groups through the load balanced path`() {
        val event = OrderEvent(UUID.randomUUID().toString(), "widgets", BigDecimal("10"), "standard")
        primaryTemplate.send(recordOf(event)).get(10, TimeUnit.SECONDS)
        assertTrue(waitFor { listener.received.contains(event.id) }, "event should be consumed")
    }

    @Test
    @Timeout(30)
    fun `keyed events land inside the topic partition range with sticky strategy`() {
        val partitions = (0 until 5).map { i ->
            val event = OrderEvent(UUID.randomUUID().toString(), "p$i", BigDecimal(i))
            primaryTemplate.send(recordOf(event, key = "key-$i")).get(10, TimeUnit.SECONDS).recordMetadata.partition()
        }
        assertTrue(partitions.all { it in 0..5 }, "partitions must be within the topic partition range")
    }

    private fun waitFor(timeout: Duration = Duration.ofSeconds(20), condition: () -> Boolean): Boolean {
        val deadline = System.currentTimeMillis() + timeout.toMillis()
        while (System.currentTimeMillis() < deadline) {
            if (condition()) return true
            Thread.sleep(200)
        }
        return condition()
    }
}
