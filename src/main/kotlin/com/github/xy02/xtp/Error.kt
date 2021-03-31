package com.github.xy02.xtp

class RemoteError(
    val className: String,
    override val message: String,
) : Throwable()

class ProtocolError(
    override val message: String = "",
) : Throwable()
