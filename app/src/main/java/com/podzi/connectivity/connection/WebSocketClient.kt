package com.podzi.connectivity.connection

import com.podzi.connectivity.queue.Message
import com.podzi.connectivity.utils.Logger
import kotlinx.coroutines.delay
import kotlin.random.Random

class WebSocketClient {
    private var isConnected: Boolean = false

    private val receivedMessages = mutableListOf<Message>()

    suspend fun connect(onSuccess: () -> Unit, onError: (Throwable) -> Unit) {
        try {
            delay(500)
            val willSucceed = Random.nextDouble() < 0.8
            if (willSucceed) {
                isConnected = true
                onSuccess()
            } else {
                throw Exception("Failed to connect to WebSocket server")
            }
        } catch (e: Exception) {
            onError(e)
        }
    }

    suspend fun sendMessage(message: Message): String {
        val willSucceed = Random.nextDouble() < 0.8
        delay(1000)
        if (willSucceed) {
            val existMessage = receivedMessages.find { it.uuid == message.uuid }
            return if (existMessage != null && existMessage.serverId != null) {
                existMessage.serverId!!
            } else {
                delay(200)
                val serverId = "srv_${Random.nextLong(1000, 9999)}"
                val newMessage = message.copy(serverId = serverId)
                receivedMessages.add(newMessage)
                serverId
            }
        }else{
            throw Exception("Failed to send message")
        }
    }

    suspend fun ping(timeoutMillis: Long): Boolean {
        val willSucceed = Random.nextDouble() < 0.9
        return if (willSucceed) {
            delay(100)
            true
        } else {
            delay(timeoutMillis)
            false
        }
    }

    suspend fun authenticate(onSuccess: () -> Unit, onError: () -> Unit) {
        if (isConnected) {
            onSuccess()
        } else {
            onError()
        }
    }
}