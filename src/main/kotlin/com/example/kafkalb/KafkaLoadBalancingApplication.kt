package com.example.kafkalb

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class KafkaLoadBalancingApplication

fun main(args: Array<String>) {
	runApplication<KafkaLoadBalancingApplication>(*args)
}
