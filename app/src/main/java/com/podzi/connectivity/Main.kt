import com.podzi.connectivity.connection.ConnectionManager
import com.podzi.connectivity.connection.WebSocketClient
import com.podzi.connectivity.queue.Message
import com.podzi.connectivity.queue.MessageQueue
import com.podzi.connectivity.state.ConnectionEvent
import com.podzi.connectivity.state.ConnectionState
import com.podzi.connectivity.utils.BackoffStrategy
import com.podzi.connectivity.utils.Logger
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.text.SimpleDateFormat
import java.util.Date
import java.util.UUID
class InteractiveCLI(
    private val connectionManager: ConnectionManager,
    private val queue: MessageQueue
) {
    private var isRunning = true

    suspend fun start() {
        printMenu()
        handleStats()

        while (isRunning) {
            print("\n> ")

            val input = readLine()?.trim() ?: continue

            if (input.isBlank()) continue

            handleCommand(input)

            if (input != "0" && input != "exit") {
                printQuickStatus()
            }
        }
    }

    private fun printQuickStatus() {
        val state = connectionManager.getCurrentState()
        val stats = queue.getStatistics()
        println("[State: ${state::class.simpleName} | Queue: ${stats.pendingMessages}P ${stats.inFlightMessages}F ${stats.sentMessages}S ${stats.failedMessages}X]")
    }

    private suspend fun handleCommand(input: String) {
        val parts = input.split(" ", limit = 2)
        val command = parts[0].lowercase()
        val args = parts.getOrNull(1)

        when (command) {
            "1", "connect" -> handleConnect()
            "2", "disconnect" -> handleDisconnect()
            "3", "send" -> handleSend(args)
            "4", "kill-heartbeat" -> handleKillHeartbeat()
            "5", "show-queue" -> handleShowQueue()
            "6", "show-state" -> handleShowState()
            "7", "reconnect" -> handleReconnect()
            "8", "clear-queue" -> handleClearQueue()
            "9", "stats" -> handleStats()
            "0", "exit" -> handleExit()
            "help" -> printMenu()
            else -> println("Unknown command: $command")
        }
    }

    private suspend fun handleConnect() {
        println("\n[ACTION] Initiating connection...")
        connectionManager.handleEvent(ConnectionEvent.ConnectRequest)
        delay(1000)
    }

    private suspend fun handleReconnect() {
        println("\n[ACTION] Forcing reconnection...")

        val currentState = connectionManager.getCurrentState()
        if (currentState is ConnectionState.Authenticated ||
            currentState is ConnectionState.Connected) {
            println("[INFO] Disconnecting first...")
            connectionManager.handleEvent(ConnectionEvent.NetworkLost)
            delay(500)
        }

        println("[INFO] Attempting to reconnect...")
        connectionManager.handleEvent(ConnectionEvent.ConnectRequest)
        delay(1000)

        println("[SUCCESS] Reconnection initiated")
    }

    private fun handleClearQueue() {
        println("\n[ACTION] Clearing message queue...")

        val stats = queue.getStatistics()
        if (stats.totalMessages == 0) {
            println("[INFO] Queue is already empty")
            return
        }
        queue.clear()
        println("[SUCCESS] Queue cleared (${stats.totalMessages} messages removed)")
    }
    private suspend fun handleDisconnect() {
        println("\n[ACTION] Simulating network loss...")
        connectionManager.handleEvent(ConnectionEvent.NetworkLost)
    }

    private fun handleSend(message: String?) {
        if (message.isNullOrBlank()) {
            println("[ERROR] Usage: send <message>")
            return
        }

        val msg = Message(
            uuid = UUID.randomUUID().toString(),
            content = message,
            timestamp = System.currentTimeMillis()
        )
        queue.enqueue(msg)
        println("[SUCCESS] Message enqueued: uuid=${msg.uuid}")
    }

    private suspend fun handleKillHeartbeat() {
        println("\n[ACTION] Simulating heartbeat timeout...")
        connectionManager.handleEvent(ConnectionEvent.HeartbeatTimeout)
    }

    private fun handleShowQueue() {
        println("\n========== Queue Details ==========")
        val allMessages = queue.getAllMessages()

        if (allMessages.isEmpty()) {
            println("Queue is empty")
        } else {
            allMessages.forEach { msg ->
                println("UUID: ${msg.uuid}")
                println("  Content: ${msg.content}")
                println("  Status: ${msg.status}")
                println("  Retry Count: ${msg.retryCount}")
                println("  Server ID: ${msg.serverId ?: "N/A"}")
                println("  Timestamp: ${formatTimestamp(msg.timestamp)}")
                println("---")
            }
        }
        println("===================================\n")
    }

    private fun handleShowState() {
        println("\n========== State Machine ==========")
        println("Current State: ${connectionManager.getCurrentState()}")
        println("===================================\n")
    }

    private fun handleStats() {
        val stats = queue.getStatistics()
        println("\n========== Statistics ==========")
        println("Total Messages: ${stats.totalMessages}")
        println("Pending: ${stats.pendingMessages}")
        println("In-Flight: ${stats.inFlightMessages}")
        println("Sent: ${stats.sentMessages}")
        println("Failed: ${stats.failedMessages}")
        println("================================\n")
    }

    private fun printMenu() {
        println("""
    
                Commands:
                  ─── Connection ───
                  1. connect          - Initiate connection
                  2. disconnect       - Simulate network loss
                  7. reconnect        - Force reconnection
                  4. kill-heartbeat   - Simulate heartbeat timeout
                  
                  ─── Messages ───
                  3. send <message>   - Enqueue new message
                  5. show-queue       - Display queue details
                  8. clear-queue      - Clear all messages
                  10. send-bulk <n>   - Send N test messages
                  
                  ─── Monitoring ───
                  6. show-state       - Display state machine
                  9. stats            - Show statistics
                  11. watch           - Auto-refresh status
                  
                  ─── System ───
                  0. exit             - Exit application
                    """.trimIndent())
                    }

    private fun handleExit() {
        println("\n[SHUTDOWN] Exiting application...")
        isRunning = false
    }

    private fun formatTimestamp(millis: Long): String {
        val date = Date(millis)
        return SimpleDateFormat("HH:mm:ss").format(date)
    }
}


fun main() = runBlocking {
    Logger.info("Application", "Application starting")

    val webSocket = WebSocketClient()
    val queue = MessageQueue()
    val backoff = BackoffStrategy()
    val connectionManager = ConnectionManager(webSocket, queue, backoff)

    queue.enqueue(Message(UUID.randomUUID().toString(), "Payment $100", System.currentTimeMillis()))
    queue.enqueue(Message(UUID.randomUUID().toString(), "Payment $200", System.currentTimeMillis()))

    val cli = InteractiveCLI(connectionManager, queue)
    cli.start()

    Logger.info("Application", "Application shutdown complete")
}