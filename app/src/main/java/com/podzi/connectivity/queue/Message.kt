package com.podzi.connectivity.queue

data class Message(
    val uuid: String,
    val content: String,
    val timestamp: Long,
    var retryCount: Int = 0,
    var status: MessageStatus = MessageStatus.PENDING,
    var serverId : String? = null
    )

enum class MessageStatus {
    PENDING,
    IN_FLIGHT,
    SENT,
    FAILED
}