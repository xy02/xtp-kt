package com.github.xy02.xtp

import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import xtp.Frame

//XTP连接，底层可使用任意有序的传输协议（例如TCP, WebSocket, QUIC）等
//data class Connection(
//    val onFrame: Observable<Frame>,
//    val frameSender: Observer<Frame>,
//)

//XTP连接，底层可使用任意有序的传输协议（例如TCP, WebSocket, QUIC）等
interface Connection{
    //当收到XTP协议数据帧
    val onFrame: Observable<Frame>
    //XTP协议数据帧的发送器
    val frameSender: Observer<Frame>
}
