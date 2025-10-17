package com.example.kafkalb.producer

import com.example.kafkalb.config.AppProperties
import com.example.kafkalb.config.KafkaAppProperties
import com.example.kafkalb.failover.ClusterHealthService
import com.example.kafkalb.failover.FailoverRouter
import com.example.kafkalb.model.OrderEvent
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.util.concurrent.TimeUnit

data class SendOutcome(
    val cluster: String,
    val strategy: String,
    val topic: String,
    val partition: Int,
    val offset: Long,
)

@Service
class OrderProducer(
    private val appProps: AppProperties,
    private val router: FailoverRouter,
    @Qualifier("primaryCustomKafkaTemplate") private val primaryCustomTemplate: KafkaTemplate<String, String>,
    @Qualifier("secondaryCustomKafkaTemplate") private val secondaryCustomTemplate: KafkaTemplate<String, String>,
) {

    private val log = LoggerFactory.getLogger(javaClass)

    fun send(record: ProducerRecord<String, String>, requestedCluster: String, strategy: String): SendOutcome {
        val cluster = router.chooseCluster(requestedCluster)
        val template: KafkaTemplate<String, String> = when {
            strategy == STRATEGY_CUSTOM && cluster == ClusterHealthService.PRIMARY -> primaryCustomTemplate
            strategy == STRATEGY_CUSTOM && cluster == ClusterHealthService.SECONDARY -> secondaryCustomTemplate
            else -> router.clusterTemplate(cluster)
        }
        val metadata = template.send(record).get(10, TimeUnit.SECONDS).recordMetadata
        val order = EventsJson.read(record.value())
        log.info(
            "sent order [{}] to cluster [{}] strategy [{}] topic {} partition {} offset {} (instance {})",
            order.id, cluster, strategy, metadata.topic(), metadata.partition(), metadata.offset(), appProps.instanceId,
        )
        return SendOutcome(cluster, strategy, metadata.topic(), metadata.partition(), metadata.offset())
    }

    companion object {
        const val STRATEGY_STICKY = "sticky"
        const val STRATEGY_CUSTOM = "custom"

        fun buildRecord(topic: String, event: OrderEvent, key: String?, partitionerStrategy: String?): ProducerRecord<String, String> {
            val json = EventsJson.write(event)
            return if (!key.isNullOrBlank() && partitionerStrategy != STRATEGY_CUSTOM) {
                ProducerRecord(topic, key, json)
            } else {
                ProducerRecord(topic, null, json)
            }
        }
    }
}
