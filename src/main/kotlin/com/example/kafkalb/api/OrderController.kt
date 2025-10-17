package com.example.kafkalb.api

import com.example.kafkalb.config.KafkaAppProperties
import com.example.kafkalb.failover.ClusterHealthService
import com.example.kafkalb.failover.FailoverRouter
import com.example.kafkalb.model.OrderEvent
import com.example.kafkalb.producer.OrderProducer
import com.example.kafkalb.producer.SendOutcome
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.math.BigDecimal
import java.util.UUID

@RestController
@RequestMapping("/api/orders")
class OrderController(
    private val producer: OrderProducer,
    private val props: KafkaAppProperties,
    private val healthService: ClusterHealthService,
) {

    private val log = LoggerFactory.getLogger(javaClass)

    data class CreateOrderRequest(
        val product: String,
        val amount: BigDecimal,
        val segment: String = "standard",
        val key: String? = null,
    )

    data class OrderResponse(
        val id: String,
        val cluster: String,
        val strategy: String,
        val topic: String,
        val partition: Int,
        val offset: Long,
    )

    data class ClusterStatus(val healthy: Boolean)

    @PostMapping
    fun create(
        @RequestBody request: CreateOrderRequest,
        @RequestParam(defaultValue = FailoverRouter.AUTO_CLUSTER) cluster: String,
        @RequestParam(defaultValue = OrderProducer.STRATEGY_STICKY) strategy: String,
    ): ResponseEntity<OrderResponse> = try {
        val event = OrderEvent(
            id = UUID.randomUUID().toString(),
            product = request.product,
            amount = request.amount,
            segment = request.segment,
        )
        val record = OrderProducer.buildRecord(props.topic.name, event, request.key, strategy)
        val outcome: SendOutcome = producer.send(record, requestedCluster = cluster, strategy = strategy)
        log.info("publish response: {}", outcome)
        ResponseEntity(
            OrderResponse(event.id, outcome.cluster, outcome.strategy, outcome.topic, outcome.partition, outcome.offset),
            HttpStatus.CREATED,
        )
    } catch (ex: Exception) {
        log.error("failed to publish order: {}", ex.message)
        ResponseEntity(HttpStatus.SERVICE_UNAVAILABLE)
    }

    @GetMapping("/clusters")
    fun clusters(): Map<String, ClusterStatus> = mapOf(
        ClusterHealthService.PRIMARY to ClusterStatus(healthService.isHealthy(ClusterHealthService.PRIMARY)),
        ClusterHealthService.SECONDARY to ClusterStatus(healthService.isHealthy(ClusterHealthService.SECONDARY)),
    )
}
