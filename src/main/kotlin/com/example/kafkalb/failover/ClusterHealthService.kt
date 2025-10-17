package com.example.kafkalb.failover

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.DescribeClusterOptions
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * Tracks primary/secondary cluster availability with a short cache so we do not
 * probe brokers on every send. A simulate flag lets tests force failover.
 */
open class ClusterHealthService(private val adminClients: Map<String, AdminClient>) {

    private val log = LoggerFactory.getLogger(javaClass)

    @Volatile
    var simulatePrimaryDown: Boolean = false

    private val cache = ConcurrentHashMap<String, HealthSnapshot>()

    open fun isHealthy(cluster: String): Boolean {
        if (cluster == PRIMARY && simulatePrimaryDown) return false
        cache[cluster]?.let { snapshot ->
            if (Instant.now().isBefore(snapshot.expiresAt)) return snapshot.healthy
        }
        val healthy = probe(cluster)
        cache[cluster] = HealthSnapshot(
            healthy = healthy,
            expiresAt = Instant.now().plus(CACHE_TTL),
        )
        return healthy
    }

    fun forceRecheck() = cache.clear()

    private fun probe(cluster: String): Boolean = try {
        val admin = adminClients.getValue(cluster)
        val nodes = admin.describeCluster(DescribeClusterOptions().timeoutMs(PROBE_TIMEOUT_MS))
            .nodes()
            .get(PROBE_TIMEOUT_MS.toLong(), TimeUnit.MILLISECONDS)
        nodes.isNotEmpty().also { healthy ->
            log.info("cluster [{}] health probe: {}", cluster, if (healthy) "UP" else "DOWN")
        }
    } catch (ex: Exception) {
        log.warn("cluster [{}] health probe failed: {}", cluster, ex.message)
        false
    }

    private data class HealthSnapshot(val healthy: Boolean, val expiresAt: Instant)

    companion object {
        const val PRIMARY = "primary"
        const val SECONDARY = "secondary"
        const val PROBE_TIMEOUT_MS = 3000
        val CACHE_TTL: Duration = Duration.ofSeconds(10)
    }
}
