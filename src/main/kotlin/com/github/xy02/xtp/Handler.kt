package com.github.xy02.xtp

import com.github.xy02.rx.getSubValues
import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.subjects.PublishSubject
import io.reactivex.rxjava3.subjects.Subject
import xtp.*

//data class Handler(
////    //XTP连接
////    val conn: Connection,
//    //处理者信息
//    val info :HandlerInfo,
//    //收到流消息
//    val onData: Observable<ByteString>,
//    //接收到对端处理者信息
//    val onRemoteHandler: Observable<RemoteHandler>,
//    //产出消息(Message.data)的发送器
//    val yieldedDataSender: Subject<ByteString>,
//    //产出HandlerInfo消息
//    val yieldHandler: (Handler)-> Unit,
//
//    val theEnd: Completable,
//)

interface Handler {
    //处理者信息
    val info :HandlerInfo
    //收到流消息
    val onData: Observable<ByteString>
    //接收到对端处理者信息
    val onRemoteHandler: Observable<RemoteHandler>
    //产出消息(Message.data)的发送器
    val yieldedDataSender: Subject<ByteString>
    //处理者生命周期结束
    val theEnd: Completable
    //产出HandlerInfo消息
    fun yieldHandler(handler:Handler)
}

internal fun newHandler(ctx: Context, handlerInfo: HandlerInfo.Builder): Handler {
    val frameSender = ctx.conn.frameSender
    val handlerId = ctx.newHandlerId()
    val info = handlerInfo.setHandlerId(handlerId).build()
    val onClose = ctx.watchCloseFrames(handlerId)
        .take(1)
        .flatMap {
            val hasError = it.close.hasError()
            if (hasError) {
                val error = it.close.error
                throw RemoteError(error.typeName,error.textMessage)
            }
            Observable.empty<Unit>()
        }
        .share()
    val yieldedDataSender = PublishSubject.create<ByteString>().toSerialized()
    val onEnd = yieldedDataSender.lastElement().toObservable().share()
    val onMessage = ctx.watchMessageFrames(handlerId)
        .map { frame -> frame.message }
        .takeUntil(onClose)
        .takeUntil(onEnd)
        .share()
    val watchMessageByType = onMessage.getSubValues { it.typeCase }
    val onRemoteHandler = watchMessageByType(Message.TypeCase.HANDLER_INFO)
        .map { newRemoteHandler(ctx, it.handlerInfo) }
        .share()
    val onData = watchMessageByType(Message.TypeCase.DATA)
        .map { it.data }
        .share()
    val availableDataSender = PublishSubject.create<ByteString>().toSerialized()
    val onSentData = availableDataSender
        .doOnNext { data ->
            val message = Message.newBuilder().setData(data)
            val frame = Frame.newBuilder().setHandlerId(handlerId)
                .setYield(message)
                .build()
            frameSender.onNext(frame)
        }
        .doOnComplete {
            val frame = Frame.newBuilder().setHandlerId(handlerId)
                .setEnd(End.getDefaultInstance())
                .build()
            frameSender.onNext(frame)
        }
        .doOnError { err->
            val error = Error.newBuilder().setTypeName(err.javaClass.name).setTextMessage(err.message?:"")
            val frame = Frame.newBuilder().setHandlerId(handlerId)
                .setEnd(End.newBuilder().setError(error))
                .build()
            frameSender.onNext(frame)
        }
        .onErrorComplete()
        .share()
    val handlerSender = PublishSubject.create<Handler>().toSerialized()
    val availableHandlerSender = PublishSubject.create<Handler>().toSerialized()
    val onSentHandler = availableHandlerSender
        .doOnNext { handler ->
            val message = Message.newBuilder().setHandlerInfo(handler.info)
            val frame = Frame.newBuilder().setHandlerId(handlerId)
                .setYield(message)
                .build()
            frameSender.onNext(frame)
        }
        .share()
    val onAvailableAmount= Observable.merge(
        onSentData.map { -1 },
        onSentHandler.flatMapSingle{ it.theEnd.toSingleDefault(-1)},
        onMessage.map { 1 },
    )
        .scan(0) { a, b -> a + b }
        .replay(1)
        .autoConnect()
    val onAvailable = onAvailableAmount.map { amount -> amount > 0 }
        .distinctUntilChanged()
        .replay(1)
        .autoConnect()
    //side effect
    yieldedDataSender
        .withLatestFrom(onAvailable) { data, ok ->
            if (ok) availableDataSender.onNext(data)
        }
        .doOnError(availableDataSender::onError)
        .doOnComplete(availableDataSender::onComplete)
        .onErrorComplete()
        .subscribe()
    handlerSender
        .withLatestFrom(onAvailable) { handler, ok ->
            if (ok) availableHandlerSender.onNext(handler)
        }
        .onErrorComplete()
        .subscribe()
//    return Handler(
//        info = info,
//        onData =onData,
//        onRemoteHandler = onRemoteHandler,
//        yieldedDataSender=yieldedDataSender,
//        yieldHandler = handlerSender::onNext,
//        theEnd = onEnd.ignoreElements(),
//    )
    return object :Handler{
        override val info: HandlerInfo
            get() = info
        override val onData: Observable<ByteString>
            get() = onData
        override val onRemoteHandler: Observable<RemoteHandler>
            get() = onRemoteHandler
        override val yieldedDataSender: Subject<ByteString>
            get() = yieldedDataSender
        override val theEnd: Completable
            get() = onEnd.ignoreElements()

        override fun yieldHandler(handler: Handler) {
            handlerSender.onNext(handler)
        }

    }
}
