package com.github.xy02.xtp

import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.subjects.SingleSubject
import xtp.Frame
import xtp.Header

//XTP端（客户端或服务端）
data class Peer(
    //XTP连接
    val conn: Connection,
    //收到对端的根流（第一个流）
    val singleRootFlow: Single<Flow>,
    //根发送通道，只有调用sendRootHeader后才会得到
    val singleRootChannel: Single<Channel>,
    //发送根流头，只发送一次，多次调用返回的都是相同的singleRootChannel
    val sendRootHeader: (header: Header.Builder) -> Single<Channel>,
)

//把xtp连接转换为Peer
fun toPeer(conn: Connection): Peer {
    val ctx = newContext(conn)
    return newPeer(ctx)
}

private fun newPeer(ctx: Context): Peer {
    val frameSender = ctx.conn.frameSender
    val newFlowId = ctx.newFlowId
    val watchMessageFrames = ctx.watchMessageFrames
    val onMessageFrame = watchMessageFrames(0)
    val firstRemoteHeader = onMessageFrame
        .map { frame -> Header.parseFrom(frame.message) }
        .doOnNext { if (it.flowId <= 0) throw ProtocolError("first flowId must greater than 0") }
        .take(1).singleOrError()
    val singleRootFlow: Single<Flow> = firstRemoteHeader.map { newFlow(ctx, it) }.cache()
    val rootHeaderSender = SingleSubject.create<Header.Builder>()
    val singleRootChannel: Single<Channel> = rootHeaderSender
        .map { it.setFlowId(newFlowId()).build() }
        .map { newChannel(ctx, it) }
        .doOnSuccess {
            //发送header
            val message = it.header.toByteString()
            val frame = Frame.newBuilder().setFlowId(0).setMessage(message).build()
            frameSender.onNext(frame)
        }
        .cache()
    //side effect
    singleRootChannel.subscribe()
    singleRootFlow.subscribe()
    onMessageFrame.subscribe()
    return Peer(
        conn = ctx.conn,
        singleRootFlow = singleRootFlow,
        singleRootChannel = singleRootChannel,
        sendRootHeader = sendRootHeader(rootHeaderSender, singleRootChannel),
    )
}

private fun sendRootHeader(
    rootHeaderSender: SingleSubject<Header.Builder>,
    singleRootChannel: Single<Channel>,
): (header: Header.Builder) -> Single<Channel> = { header ->
    rootHeaderSender.onSuccess(header)
    singleRootChannel
}
