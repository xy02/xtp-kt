package com.github.xy02.xtp

class RemoteError(
    val typeName: String,
    override val message: String,
) : Throwable()

class ProtocolError(
    override val message: String = "",
) : Throwable()
