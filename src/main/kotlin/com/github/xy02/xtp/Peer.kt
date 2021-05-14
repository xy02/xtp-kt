package com.github.xy02.xtp

import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.subjects.SingleSubject
import xtp.Frame
import xtp.HandlerInfo
import xtp.Message

//XTP端（客户端或服务端）
//data class Peer(
//    //XTP连接
//    val conn: Connection,
//    val ctx: Context,
//    //收到对端的根流（第一个流）
//    val singleRootFlow: Single<Flow>,
//    //根发送通道，只有调用sendRootHeader后才会得到
//    val singleRootChannel: Single<Channel>,
//    //发送根流头，只发送一次，多次调用返回的都是相同的singleRootChannel
//    val sendRootHeader: (header: Header.Builder) -> Single<Channel>,
//)

//XTP端（客户端或服务端）
interface Peer {
    //收到对端的根处理者
    val onRootRemoteHandler: Single<RemoteHandler>

    //创建处理者
    fun createHandler(handlerInfo: HandlerInfo.Builder): Handler

    //声明根处理者信息
    fun declareRootHandlerInfo(handler: Handler)

    //关闭连接
    fun close()
}

//把xtp连接转换为Peer
fun toPeer(conn: Connection): Peer {
    val ctx = newContext(conn)
    return newPeer(ctx)
}

private fun newPeer(ctx: Context): Peer {
    val frameSender = ctx.conn.frameSender
    val onMessageFrame = ctx.watchMessageFrames(0)
    val firstRemoteHandler = onMessageFrame
        .map { frame ->
            val message = frame.message
            if (message.typeCase != Message.TypeCase.HANDLER_INFO)
                throw ProtocolError("first message type must be HANDLER_INFO")
            val info = message.handlerInfo
            if (info.handlerId <= 0)
                throw ProtocolError("first handlerId must greater than 0")
            newRemoteHandler(ctx, info)
        }
        .take(1)
        .singleOrError()
        .cache()
    val rootHandlerInfoSender = SingleSubject.create<Handler>()
    val singleRootHandler = rootHandlerInfoSender
        .doOnSuccess {
            //发送handlerInfo
            val message = Message.newBuilder().setHandlerInfo(it.info)
            val frame = Frame.newBuilder().setHandlerId(0)
                .setMessage(message).build()
            frameSender.onNext(frame)
        }
        .cache()
    //side effect
    singleRootHandler.subscribe()
    firstRemoteHandler.subscribe()
    onMessageFrame.subscribe()
    return object : Peer {
        override val onRootRemoteHandler: Single<RemoteHandler>
            get() = firstRemoteHandler

        override fun createHandler(handlerInfo: HandlerInfo.Builder): Handler {
            return newHandler(ctx, handlerInfo)
        }

        override fun declareRootHandlerInfo(handler: Handler) {
            rootHandlerInfoSender.onSuccess(handler)
        }

        override fun close() {
            ctx.conn.frameSender.onComplete()
        }
    }
}
