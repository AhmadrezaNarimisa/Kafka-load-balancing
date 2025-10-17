package com.example.kafkalb.failover

import com.example.kafkalb.model.OrderEvent
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

/**
 * Picks the KafkaTemplate that matches the requested cluster while honouring
 * automatic failover: when the primary cluster is unhealthy, traffic is routed
 * to the secondary cluster instead.
 */
@Component
class FailoverRouter(
    private val health: ClusterHealthService,
    @Qualifier("primaryKafkaTemplate") private val primaryTemplate: KafkaTemplate<String, String>,
    @Qualifier("secondaryKafkaTemplate") private val secondaryTemplate: KafkaTemplate<String, String>,
) {

    private val log = LoggerFactory.getLogger(javaClass)

    fun clusterTemplate(cluster: String): KafkaTemplate<String, String> = when (cluster) {
        ClusterHealthService.PRIMARY -> primaryTemplate
        ClusterHealthService.SECONDARY -> secondaryTemplate
        else -> throw IllegalArgumentException("unknown cluster: $cluster")
    }

    /** Returns the cluster that should be used for the next send. */
    fun chooseCluster(requested: String): String = when (requested) {
        ClusterHealthService.PRIMARY -> ClusterHealthService.PRIMARY
        ClusterHealthService.SECONDARY -> ClusterHealthService.SECONDARY
        AUTO -> when {
            health.isHealthy(ClusterHealthService.PRIMARY) -> ClusterHealthService.PRIMARY
            health.isHealthy(ClusterHealthService.SECONDARY) -> {
                log.warn("primary cluster down - failing over to secondary")
                ClusterHealthService.SECONDARY
            }
            else -> throw IllegalStateException("no healthy Kafka cluster available: primary and secondary both unhealthy")
        }
        else -> throw IllegalArgumentException("unknown cluster: $requested")
    }

    companion object {
        const val AUTO = "auto"
        const val AUTO_CLUSTER = "auto"
    }
}
