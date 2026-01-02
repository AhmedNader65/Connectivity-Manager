package com.podzi.connectivity.connection

import com.podzi.connectivity.queue.MessageQueue
import com.podzi.connectivity.state.ConnectionEvent
import com.podzi.connectivity.state.ConnectionState
import com.podzi.connectivity.utils.BackoffStrategy
import com.podzi.connectivity.utils.Logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

const val TAG = "ConnectionManager"
class ConnectionManager(
    private val webSocketClient: WebSocketClient,
    private val queue: MessageQueue,
    private val backoff: BackoffStrategy
) {
    companion object {
        const val MAX_SEND_RETRIES = 3
        const val MAX_CONNECTION_RETRIES = 3

        const val BATCH_SIZE = 3

        const val HEARTBEAT_INTERVAL_MS = 15_000L

        const val HEARTBEAT_TIMEOUT_MS = 5_000L
    }

    val coroutineScope = CoroutineScope(SupervisorJob() + kotlinx.coroutines.Dispatchers.IO)
    private var currentState: ConnectionState = ConnectionState.Disconnected

    fun getCurrentState(): ConnectionState {
        return currentState
    }

    suspend fun handleEvent(event: ConnectionEvent) {
        Logger.debug(
            TAG,
            "Received event: ${event::class.simpleName}, currentState=${currentState::class.simpleName}"
        )
        when (event) {
            is ConnectionEvent.ConnectRequest -> {
                Logger.info(
                    TAG,
                    "State transition: ${currentState::class.simpleName} -> Connecting"
                )
                currentState = ConnectionState.Connecting
                Logger.info(TAG, "Attempting  WebSocket connection...")
                try {
                    attemptConnect()
                } catch (e: Exception) {
                    Logger.error(
                        TAG,
                        "Exception during connection attempt: ${e.message}",
                        e
                    )
                    currentState = ConnectionState.Disconnected
                }
            }

            is ConnectionEvent.NetworkLost -> {
                Logger.warn(
                    TAG,
                    "Network connection lost, currentState=${currentState::class.simpleName}, transitioning to Disconnected"
                )
                currentState = ConnectionState.Reconnecting(attempt = 1)
                Logger.info(TAG, "Initiating reconnection after network loss")
                attemptReconnect(attempt = 1)
            }

            is ConnectionEvent.HeartbeatTimeout -> {
                Logger.warn(
                    TAG,
                    "Heartbeat timeout detected, currentState=${currentState::class.simpleName}, transitioning to Disconnected"
                )
                currentState = ConnectionState.Disconnected
                Logger.info(TAG, "Initiating reconnection after heartbeat timeout")
                handleEvent(ConnectionEvent.ConnectRequest)
            }

            is ConnectionEvent.AuthSucceed -> {
                Logger.info(
                    TAG,
                    "Authentication succeeded, starting heartbeat and message processing"
                )
                queue.resetInFlightToPending()
                Logger.debug(TAG, "Starting heartbeat monitor")
                startHeartbeat()
                Logger.debug(TAG, "Starting message processing loop")
                startMessageProcessing()
            }
        }
    }

    private suspend fun attemptReconnect(attempt: Int) {
        val delay = backoff.getDelay(attempt)
        Logger.info(TAG, "Reconnecting in ${delay}ms (attempt $attempt)")
        delay(delay)
        coroutineScope.launch {
            attemptConnect(attempt)
        }
    }

    private fun startMessageProcessing() {
        coroutineScope.launch {
            Logger.debug(TAG, "Message processing loop active")
            while (currentState == ConnectionState.Authenticated) {
                val messages = queue.getPendingMessages()

                if (messages.isNullOrEmpty()) {
                    delay(500)
                    continue
                }

                messages.chunked(BATCH_SIZE).forEach { batch ->
                    batch.forEach { message ->
                        coroutineScope.launch {
                            queue.markAsInFlight(message.uuid)

                            try {
                                val serverId = webSocketClient.sendMessage(message)
                                queue.markAsSent(message.uuid, serverId)
                            } catch (e: Exception) {
                                queue.incrementRetryCount(message.uuid)
                            }
                        }
                    }
                }

                delay(1000)
            }

        }
    }

    private fun startHeartbeat() {
        coroutineScope.launch {
            Logger.debug(
                TAG,
                "Heartbeat monitor started, currentState=${currentState::class.simpleName}"
            )
            while (currentState == ConnectionState.Authenticated) {
                Logger.info(TAG, "Sending heartbeat ping...")
                val pingSuccess = webSocketClient.ping(timeoutMillis = HEARTBEAT_TIMEOUT_MS)
                Logger.info0(
                    TAG,
                    "Heartbeat ping result: ${if (pingSuccess) "success" else "failure"}"
                )
                if (!pingSuccess) {
                    Logger.error(
                        TAG,
                        "Heartbeat ping failed, transitioning to Disconnected"
                    )
                    currentState = ConnectionState.Disconnected
                    Logger.info(TAG, "Heartbeat timeout triggered reconnection")
                    handleEvent(ConnectionEvent.HeartbeatTimeout)
                    return@launch
                }
                delay(HEARTBEAT_INTERVAL_MS)
            }
        }
    }

    private suspend fun attemptConnect(attempt: Int = 0) {
        webSocketClient.connect(
            onSuccess = {
                currentState = ConnectionState.Connected
                Logger.info(
                    TAG,
                    "WebSocket connected successfully, state=${currentState::class.simpleName}"
                )
                coroutineScope.launch {
                    Logger.info(TAG, "Initiating authentication...")
                    attemptAuthentication()
                }
            }, onError = {
                coroutineScope.launch {
                    if (attempt < MAX_CONNECTION_RETRIES) {
                        val delay = backoff.getDelay(attempt)
                        Logger.info(
                            TAG,
                            "Reconnecting in ${delay}ms (attempt $attempt)"
                        )
                        delay(delay)
                        attemptConnect(attempt + 1)
                    } else {
                        Logger.error(TAG, "Max connection retries exceeded")
                        currentState = ConnectionState.Disconnected
                    }
                }
            }
        )
    }

    private suspend fun attemptAuthentication() {
        webSocketClient.authenticate(
            onSuccess = {
                currentState = ConnectionState.Authenticated
                Logger.info(
                    TAG,
                    "Authentication successful, state=${currentState::class.simpleName}"
                )
                coroutineScope.launch {
                    handleEvent(ConnectionEvent.AuthSucceed)
                }
            }, onError = {
                Logger.error(
                    TAG,
                    "Authentication failed, state=${currentState::class.simpleName}"
                )
                currentState = ConnectionState.Disconnected
            }
        )
    }
}