package com.example.kafkalb.partitioner

import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.Cluster
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SegmentPartitionerTest {

    private val partitioner = SegmentPartitioner()

    private fun clusterWithSixPartitions(): Cluster {
        val nodes = List(3) { Node(it, "host$it", 9092) }
        val partitions = (0 until 6).map { PartitionInfo("orders-topic", it, nodes[it % 3], null, null) }
        return Cluster("test", nodes.toSet(), partitions, emptySet(), emptySet())
    }

    @Test
    fun `premium keys are pinned to partition 0`() {
        val cluster = clusterWithSixPartitions()
        repeat(10) { i ->
            val partition = partitioner.partition("orders-topic", "premium-$i", null, null, null, cluster)
            assertEquals(0, partition)
        }
    }

    @Test
    fun `keyless messages spread round-robin across all partitions`() {
        val cluster = clusterWithSixPartitions()
        val assigned = (0 until 30).map {
            partitioner.partition("orders-topic", null, null, null, null, cluster)
        }
        assertEquals((0..5).toSet(), assigned.toSet())
    }
}
