package com.example.kafkalb.config

import com.example.kafkalb.consumer.RebalanceLoggingListener
import com.example.kafkalb.failover.ClusterHealthService
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate

@Configuration
class KafkaClusterConfig {

    private fun template(props: KafkaAppProperties, cluster: String, customPartitioner: Boolean): KafkaTemplate<String, String> {
        val bootstrapServers = props.clusters.getValue(cluster).bootstrapServers
        val producerProps = mutableMapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
        )
        if (customPartitioner) {
            producerProps[ProducerConfig.PARTITIONER_CLASS_CONFIG] = com.example.kafkalb.partitioner.SegmentPartitioner::class.java
        }
        return KafkaTemplate(DefaultKafkaProducerFactory(producerProps))
    }

    @Bean
    fun primaryKafkaTemplate(props: KafkaAppProperties): KafkaTemplate<String, String> =
        template(props, "primary", customPartitioner = false)

    @Bean
    fun secondaryKafkaTemplate(props: KafkaAppProperties): KafkaTemplate<String, String> =
        template(props, "secondary", customPartitioner = false)

    @Bean
    fun primaryCustomKafkaTemplate(props: KafkaAppProperties): KafkaTemplate<String, String> =
        template(props, "primary", customPartitioner = true)

    @Bean
    fun secondaryCustomKafkaTemplate(props: KafkaAppProperties): KafkaTemplate<String, String> =
        template(props, "secondary", customPartitioner = true)

    @Bean
    fun primaryAdminClient(props: KafkaAppProperties): AdminClient =
        adminClient(props.clusters.getValue("primary").bootstrapServers)

    @Bean
    fun secondaryAdminClient(props: KafkaAppProperties): AdminClient =
        adminClient(props.clusters.getValue("secondary").bootstrapServers)

    private fun adminClient(bootstrapServers: String): AdminClient = AdminClient.create(
        mapOf(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG to 3000,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG to 3000,
        ),
    )

    private fun consumerContainerFactory(
        props: KafkaAppProperties,
        appProps: AppProperties,
        groupId: String,
    ): ConcurrentKafkaListenerContainerFactory<String, String> {
        val consumerFactory = DefaultKafkaConsumerFactory<String, String>(
            mapOf<String, Any>(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to props.clusters.getValue(appProps.consumeFrom).bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to groupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ),
        )
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.setConsumerFactory(consumerFactory)
        factory.setConcurrency(appProps.listenerConcurrency)
        factory.containerProperties.setConsumerRebalanceListener(
            RebalanceLoggingListener(appProps.instanceId),
        )
        return factory
    }

    @Bean
    fun billingContainerFactory(props: KafkaAppProperties, appProps: AppProperties): ConcurrentKafkaListenerContainerFactory<String, String> =
        consumerContainerFactory(props, appProps, GROUP_BILLING)

    @Bean
    fun analyticsContainerFactory(props: KafkaAppProperties, appProps: AppProperties): ConcurrentKafkaListenerContainerFactory<String, String> =
        consumerContainerFactory(props, appProps, GROUP_ANALYTICS)

    @Bean
    fun ordersTopic(props: KafkaAppProperties): NewTopic =
        NewTopic(props.topic.name, props.topic.partitions, props.topic.replicas.toShort())

    @Bean
    fun kafkaAdmin(props: KafkaAppProperties): KafkaAdmin = KafkaAdmin(
        mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to props.clusters.getValue("primary").bootstrapServers),
    )

    @Bean
    fun clusterHealthService(
        @Qualifier("primaryAdminClient") primary: AdminClient,
        @Qualifier("secondaryAdminClient") secondary: AdminClient,
    ): ClusterHealthService = ClusterHealthService(
        mapOf("primary" to primary, "secondary" to secondary),
    )

    companion object {
        const val GROUP_BILLING = "billing"
        const val GROUP_ANALYTICS = "analytics"
    }
}
