package com.example.kafkalb.config

import org.springframework.boot.context.properties.ConfigurationProperties

data class ClusterEndpoint(val bootstrapServers: String)

data class TopicSpec(
    val name: String = "orders-topic",
    val partitions: Int = 6,
    val replicas: Int = 1,
)

@ConfigurationProperties(prefix = "kafka")
data class KafkaAppProperties(
    val clusters: Map<String, ClusterEndpoint> = emptyMap(),
    val topic: TopicSpec = TopicSpec(),
)

@ConfigurationProperties(prefix = "app")
data class AppProperties(
    val instanceId: String = "instance-1",
    val consumeFrom: String = "primary",
    val listenerConcurrency: Int = 1,
)
