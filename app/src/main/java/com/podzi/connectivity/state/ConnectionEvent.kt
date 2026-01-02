package com.podzi.connectivity.state

sealed class ConnectionEvent {
    object ConnectRequest : ConnectionEvent()
    object AuthSucceed : ConnectionEvent()
    object NetworkLost : ConnectionEvent()
    object HeartbeatTimeout : ConnectionEvent()
}