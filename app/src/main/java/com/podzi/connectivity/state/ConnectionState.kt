package com.podzi.connectivity.state

sealed class ConnectionState {
    object Connected : ConnectionState()
    object Disconnected : ConnectionState()
    data class Reconnecting(val attempt: Int) : ConnectionState()
    object Authenticated : ConnectionState()
    object Connecting : ConnectionState()
    object SUSPENDED : ConnectionState()
}