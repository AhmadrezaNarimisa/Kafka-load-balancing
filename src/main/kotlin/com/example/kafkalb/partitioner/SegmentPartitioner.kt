package com.example.kafkalb.partitioner

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import java.util.concurrent.atomic.AtomicInteger

/**
 * Custom partitioning strategy demonstrating producer-side load balancing:
 * - keys starting with "premium-" are pinned to partition 0
 * - everything else (including keyless messages) is rotated round-robin
 */
class SegmentPartitioner : Partitioner {

    private val counter = AtomicInteger()

    override fun partition(
        topic: String,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any?,
        valueBytes: ByteArray?,
        cluster: Cluster,
    ): Int = when {
        key is String && key.startsWith(PREMIUM_PREFIX) -> 0
        else -> counter.getAndIncrement() % partitionsOf(cluster, topic)
    }

    private fun partitionsOf(cluster: Cluster, topic: String): Int =
        maxOf(1, cluster.partitionsForTopic(topic).size)

    override fun close() {}

    override fun configure(configs: Map<String, *>) {}

    companion object {
        const val PREMIUM_PREFIX = "premium-"
    }
}
