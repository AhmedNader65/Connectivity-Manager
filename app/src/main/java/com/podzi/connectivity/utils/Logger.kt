package com.podzi.connectivity.utils

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

object Logger {
    private val dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.US)

    enum class Level {
        DEBUG, INFO, WARN, ERROR
    }

    private fun log(level: Level, tag: String, message: String, throwable: Throwable? = null) {
        val timestamp = dateFormat.format(Date())
        val logMessage = "[$timestamp] [${level.name}] [$tag] $message"
        if (level == Level.DEBUG)
            return
        println(logMessage)
        throwable?.let {
            println("${it.javaClass.simpleName}: ${it.message}")
            it.printStackTrace()
        }
    }

    fun debug(tag: String, message: String) = log(Level.DEBUG, tag, message)
    fun info(tag: String, message: String) = log(Level.INFO, tag, message)
    fun warn(tag: String, message: String, throwable: Throwable? = null) =
        log(Level.WARN, tag, message, throwable)

    fun error(tag: String, message: String, throwable: Throwable? = null) =
        log(Level.ERROR, tag, message, throwable)
}
