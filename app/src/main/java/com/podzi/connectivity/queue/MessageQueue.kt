package com.podzi.connectivity.queue

import com.podzi.connectivity.connection.ConnectionManager.Companion.MAX_SEND_RETRIES
import com.podzi.connectivity.utils.Logger
import java.util.concurrent.ConcurrentLinkedQueue

class MessageQueue {
    private val messages = ConcurrentLinkedQueue<Message>()

    fun enqueue(message: Message) {
        Logger.debug(
            "MessageQueue",
            "Enqueuing message: uuid=${message.uuid}, timestamp=${message.timestamp}, status=${message.status}"
        )
        messages.add(message)
        Logger.debug("MessageQueue", "Message added to queue, queue size=${messages.size}")
    }

    fun getPendingMessages(): List<Message>? {
        val pendingMessages = messages.filter { it.status == MessageStatus.PENDING }
        if (pendingMessages.isNotEmpty()) {
            Logger.info("MessageQueue", "Found ${pendingMessages.size} pending messages")
        }
        return pendingMessages
    }

    fun markAsSent(uuid: String, serverId: String) {
        Logger.debug("MessageQueue", "Marking message as sent: uuid=$uuid")
        val message = messages.find { it.uuid == uuid }
        message?.let {
            it.status = MessageStatus.SENT
            it.serverId = serverId
            Logger.info(
                "MessageQueue",
                "Message marked as SENT: uuid=$uuid, serverId=$serverId, status=SENT"
            )
        } ?: Logger.warn("MessageQueue", "Cannot mark as sent, message not found: uuid=$uuid")
    }

    fun incrementRetryCount(uuid: String) {
        Logger.debug("MessageQueue", "Incrementing retry count for message: uuid=$uuid")
        val message = messages.find { it.uuid == uuid }
        message?.let {
            Logger.debug(
                "MessageQueue",
                "Current retry count: ${it.retryCount} for uuid=${it.uuid}"
            )
            it.retryCount += 1
            it.status = MessageStatus.PENDING
            Logger.info(
                "MessageQueue",
                "Retry count incremented: uuid=${it.uuid}, retryCount=${it.retryCount}"
            )
            if (it.retryCount >= MAX_SEND_RETRIES) {
                markAsFailed(uuid)
            }
        } ?: Logger.warn(
            "MessageQueue",
            "Cannot increment retry count, message not found: uuid=$uuid"
        )
    }

    fun markAsFailed(uuid: String) {
        Logger.debug("MessageQueue", "Marking message as failed: uuid=$uuid")
        val message = messages.find { it.uuid == uuid }
        message?.let {
            it.status = MessageStatus.FAILED
            Logger.error("MessageQueue", "Message marked as FAILED: uuid=${it.uuid}, status=FAILED")
        } ?: Logger.warn("MessageQueue", "Cannot mark as failed, message not found: uuid=$uuid")
    }

    fun hasNoPendings(): Boolean {
        val noPendings = messages.none { it.status == MessageStatus.PENDING }
        Logger.debug("MessageQueue", "Checking for pending messages: hasNoPendings=$noPendings")
        return noPendings
    }

    fun markAsInFlight(uuid: String) {
        Logger.debug("MessageQueue", "Marking message as in-flight: uuid=$uuid")
        val message = messages.find { it.uuid == uuid }
        message?.let {
            it.status = MessageStatus.IN_FLIGHT
            Logger.info(
                "MessageQueue",
                "Message marked as IN_FLIGHT: uuid=${it.uuid}, status=IN_FLIGHT"
            )
        } ?: Logger.warn("MessageQueue", "Cannot mark as in-flight, message not found: uuid=$uuid")
    }

    fun resetInFlightToPending() {
        messages.filter { it.status == MessageStatus.IN_FLIGHT }
            .forEach { it.status = MessageStatus.PENDING }
    }

    fun getStatistics(): Statistics {
        val totalMessages = messages.size
        val pendingMessages = messages.count { it.status == MessageStatus.PENDING }
        val inFlightMessages = messages.count { it.status == MessageStatus.IN_FLIGHT }
        val sentMessages = messages.count { it.status == MessageStatus.SENT }
        val failedMessages = messages.count { it.status == MessageStatus.FAILED }

        return Statistics(
            totalMessages = totalMessages,
            pendingMessages = pendingMessages,
            inFlightMessages = inFlightMessages,
            sentMessages = sentMessages,
            failedMessages = failedMessages
        )
    }

    fun getAllMessages(): List<Message> {
        return messages.toList()
    }

    fun clear() {
        messages.clear()
    }

    data class Statistics(
        val totalMessages: Int,
        val pendingMessages: Int,
        val inFlightMessages: Int,
        val sentMessages: Int,
        val failedMessages: Int
    )
}