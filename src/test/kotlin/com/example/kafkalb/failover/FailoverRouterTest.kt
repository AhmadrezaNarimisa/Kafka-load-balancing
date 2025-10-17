package com.example.kafkalb.failover

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate

class FailoverRouterTest {

    private val templates = mapOf(
        ClusterHealthService.PRIMARY to KafkaTemplate<String, String>(DefaultKafkaProducerFactory(mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:1"))),
        ClusterHealthService.SECONDARY to KafkaTemplate<String, String>(DefaultKafkaProducerFactory(mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:2"))),
    )
    private fun router(health: ClusterHealthService): FailoverRouter =
        FailoverRouter(health, templates.getValue(ClusterHealthService.PRIMARY), templates.getValue(ClusterHealthService.SECONDARY))

    @Test
    fun `explicit cluster requests bypass failover`() {
        val health = ClusterHealthService(mapOf())
        health.simulatePrimaryDown = true
        health.forceRecheck()
        assertEquals(ClusterHealthService.PRIMARY, router(health).chooseCluster(ClusterHealthService.PRIMARY))
        assertEquals(ClusterHealthService.SECONDARY, router(health).chooseCluster(ClusterHealthService.SECONDARY))
    }

    @Test
    fun `auto routes to primary when it is healthy`() {
        val health = FakeHealthyService()
        assertEquals(ClusterHealthService.PRIMARY, router(health).chooseCluster(FailoverRouter.AUTO))
    }

    @Test
    fun `auto fails over to secondary when primary is down`() {
        val health = FakeHealthyService()
        health.simulatePrimaryDown = true
        assertEquals(ClusterHealthService.SECONDARY, router(health).chooseCluster(FailoverRouter.AUTO))
    }

    @Test
    fun `auto throws when both clusters are down`() {
        val health = object : ClusterHealthService(mapOf()) {
            override fun isHealthy(cluster: String): Boolean = false
        }
        assertThrows(IllegalStateException::class.java) { router(health).chooseCluster(FailoverRouter.AUTO) }
    }

    @Test
    fun `cluster template resolution maps correctly`() {
        val router = router(FakeHealthyService())
        assertEquals(templates.getValue(ClusterHealthService.PRIMARY), router.clusterTemplate(ClusterHealthService.PRIMARY))
        assertEquals(templates.getValue(ClusterHealthService.SECONDARY), router.clusterTemplate(ClusterHealthService.SECONDARY))
    }

    private class FakeHealthyService : ClusterHealthService(mapOf()) {
        override fun isHealthy(cluster: String): Boolean = !(cluster == ClusterHealthService.PRIMARY && simulatePrimaryDown)
    }
}
