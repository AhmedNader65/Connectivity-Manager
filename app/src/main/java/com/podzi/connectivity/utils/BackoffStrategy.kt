package com.podzi.connectivity.utils

class BackoffStrategy(private val baseDelayMs: Long = 1000L, private val maxDelayMs: Long = 32000L) {
    fun getDelay(attempt: Int): Long {
        Logger.debug("BackoffStrategy", "Calculating backoff delay for attempt=$attempt")
        val expDelay = baseDelayMs * (1L shl attempt.coerceAtMost(5))
        Logger.debug("BackoffStrategy", "Exponential delay calculated: ${expDelay}ms (base=${baseDelayMs}ms, exponent=${attempt.coerceAtMost(5)})")
        val jitter = (0..baseDelayMs / 2).random()
        Logger.debug("BackoffStrategy", "Jitter added: ${jitter}ms (range: 0-${baseDelayMs / 2}ms)")
        val finalDelay = (expDelay + jitter).coerceAtMost(maxDelayMs)
        Logger.info("BackoffStrategy", "Backoff delay: attempt=$attempt, delay=${finalDelay}ms (max=${maxDelayMs}ms)")
        return finalDelay
    }
}