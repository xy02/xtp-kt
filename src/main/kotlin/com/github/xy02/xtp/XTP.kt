package com.github.xy02.xtp

class RemoteError(
    val type: String,
    override val message: String,
) : Throwable()

class ProtocolError(
    override val message: String = "",
) : Throwable()
