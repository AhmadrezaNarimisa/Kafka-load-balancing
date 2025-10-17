package com.example.kafkalb.producer

import com.example.kafkalb.model.OrderEvent
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule
import tools.jackson.module.kotlin.readValue

/** Single shared Jackson 3 mapper so producers and consumers agree on JSON shape. */
object EventsJson {

    private val mapper: JsonMapper = JsonMapper.builder()
        .addModule(KotlinModule.Builder().build())
        .build()

    fun write(event: OrderEvent): String = mapper.writeValueAsString(event)

    fun read(json: String): OrderEvent = mapper.readValue(json)
}
