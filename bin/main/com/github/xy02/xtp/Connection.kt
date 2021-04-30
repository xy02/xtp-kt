package com.github.xy02.xtp

import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import xtp.Frame

//XTP连接，底层可使用任意有序的传输协议（例如TCP, WebSocket, QUIC）等
data class Connection(
    val onFrame: Observable<Frame>,
    val frameSender: Observer<Frame>,
)

//关闭连接
fun close(conn: Connection) {
    conn.frameSender.onComplete()
}