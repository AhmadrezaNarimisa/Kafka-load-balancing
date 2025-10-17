package com.example.kafkalb.consumer

import com.example.kafkalb.config.AppProperties
import com.example.kafkalb.config.KafkaClusterConfig
import com.example.kafkalb.model.OrderEvent
import com.example.kafkalb.producer.EventsJson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

@Component
class OrderEventListener(private val appProps: AppProperties) {

    private val log = LoggerFactory.getLogger(javaClass)

    /** All consumed event ids, shared by both consumer groups. */
    val received = ConcurrentHashMap.newKeySet<String>()

    @KafkaListener(topics = ["\${kafka.topic.name}"], containerFactory = "billingContainerFactory")
    fun billing(record: ConsumerRecord<String, String>) = consume(record, KafkaClusterConfig.GROUP_BILLING)

    @KafkaListener(topics = ["\${kafka.topic.name}"], containerFactory = "analyticsContainerFactory")
    fun analytics(record: ConsumerRecord<String, String>) = consume(record, KafkaClusterConfig.GROUP_ANALYTICS)

    private fun consume(record: ConsumerRecord<String, String>, group: String) {
        val order = runCatching { EventsJson.read(record.value()) }.getOrNull()
        log.info(
            "instance [{}] group [{}] consumed [{}] from partition {} at offset {}",
            appProps.instanceId, group, order?.id ?: record.value(), record.partition(), record.offset(),
        )
        order?.let { received.add(it.id) }
    }
}
