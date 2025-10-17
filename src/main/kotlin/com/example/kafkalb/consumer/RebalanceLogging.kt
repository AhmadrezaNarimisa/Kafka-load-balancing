package com.example.kafkalb.consumer

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

/**
 * Logs partition assignment during rebalances so you can watch Kafka balance
 * load across consumer group members in the application logs.
 */
class RebalanceLoggingListener(private val instanceId: String) : ConsumerRebalanceListener {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
        logPartitionEvent("REVOKED", partitions)
    }

    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
        logPartitionEvent("ASSIGNED", partitions)
    }

    override fun onPartitionsLost(partitions: Collection<TopicPartition>) {
        logPartitionEvent("LOST", partitions)
    }

    private fun logPartitionEvent(event: String, partitions: Collection<TopicPartition>) {
        log.info(
            "instance [{}] rebalance {}: partitions {}",
            instanceId, event, partitions.joinToString { "${it.topic()}#${it.partition()}" },
        )
    }
}
