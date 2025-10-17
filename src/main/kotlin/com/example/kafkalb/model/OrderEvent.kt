package com.example.kafkalb.model

import java.math.BigDecimal
import java.time.Instant

data class OrderEvent(
    val id: String,
    val product: String,
    val amount: BigDecimal,
    val segment: String = "standard",
    val createdAt: Instant = Instant.now(),
)
