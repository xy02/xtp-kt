package com.github.xy02.xtp

import com.github.xy02.rx.getSubValues
import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.subjects.PublishSubject
import io.reactivex.rxjava3.subjects.Subject
import xtp.End
import xtp.Error
import xtp.Frame
import xtp.HandlerInfo
import xtp.Message

//data class RemoteHandler(
//    //处理者信息
//    val info : HandlerInfo,
//    //消息(Message.data)的发送器
//    val dataSender: Subject<ByteString>,
//    //发送HandlerInfo消息
//    val sendHandlerInfo: (HandlerInfo)-> Unit,
//    //收到的产出消息
//    val onYieldedData: Observable<ByteString>,
//    //接收到对端产出的处理者信息
//    val onRemoteHandler: Observable<RemoteHandler>,
//
//    val theEnd: Completable,
//)
interface RemoteHandler {
    //处理者信息
    val info : HandlerInfo
    //消息(Message.data)的发送器
    val dataSender: Subject<ByteString>
    //收到的产出消息
    val onYieldedData: Observable<ByteString>
    //接收到对端产出的处理者信息
    val onRemoteHandler: Observable<RemoteHandler>

    val theEnd: Completable
    //发送HandlerInfo消息
    fun sendHandlerInfo (info :HandlerInfo)
}

internal fun newRemoteHandler(ctx:Context,handlerInfo: HandlerInfo):RemoteHandler{
    val frameSender = ctx.conn.frameSender
    val handlerId = handlerInfo.handlerId
    val send = { fb:Frame.Builder->
        val frame = fb.setHandlerId(handlerId).build()
        frameSender.onNext(frame)
    }
    val onEnd = ctx.watchEndFrames(handlerId)
        .take(1)
        .flatMap {
            val hasError = it.end.hasError()
            if (hasError) {
                val error = it.end.error
                throw RemoteError(error.typeName,error.textMessage)
            }
            Observable.empty<Unit>()
        }
        .share()
    val onYield = ctx.watchYieldFrames(handlerId)
        .map { frame -> frame.yield }

        .takeUntil(onEnd)
        .share()
    val watchYieldByType = onYield.getSubValues { it.typeCase }
    val onRemoteHandler = watchYieldByType(Message.TypeCase.HANDLER_INFO)
        .map { newRemoteHandler(ctx, it.handlerInfo) }
        .share()
    val onYieldedData = watchYieldByType(Message.TypeCase.DATA)
        .map { it.data }
        .share()
    val dataSender = PublishSubject.create<ByteString>().toSerialized()
    val handlerInfoSender = PublishSubject.create<HandlerInfo>().toSerialized()
    val availableDataSender = PublishSubject.create<ByteString>().toSerialized()
    val onSentData = availableDataSender
        .doOnNext { data ->
            val message = Message.newBuilder().setData(data)
            val fb = Frame.newBuilder().setMessage(message)
            send(fb)
        }
        .doOnComplete {
            val fb = Frame.newBuilder().setEnd(End.getDefaultInstance())
            send(fb)
        }
        .doOnError { err->
            val error = Error.newBuilder().setTypeName(err.javaClass.name).setTextMessage(err.message?:"")
            val fb = Frame.newBuilder().setEnd(End.newBuilder().setError(error))
            send(fb)
        }
        .onErrorComplete()
        .share()
    val availableHandlerSender = PublishSubject.create<HandlerInfo>().toSerialized()
    val onSentHandlerInfo = availableHandlerSender
        .doOnNext { info->
            val message = Message.newBuilder().setHandlerInfo(info)
            val fb = Frame.newBuilder().setMessage(message)
            send(fb)
        }
        .share()
    val onSentMessage = Observable.merge(
        onSentHandlerInfo,
        onSentData
    )
    val onAvailableAmount = Observable.merge(
        Observable.just(handlerInfo.channelSize),
        onSentMessage.map { -1 },
        onYieldedData.map { 1 },
        onRemoteHandler.flatMapSingle{ it.theEnd.toSingleDefault(1)},
    ).scan(0) { a, b -> a + b }
        .replay(1)
        .autoConnect()
    val onAvailable = onAvailableAmount.map { amount -> amount > 0 }
        .distinctUntilChanged()
        .replay(1)
        .autoConnect()
    //side effect
    dataSender
        .withLatestFrom(onAvailable) { data, ok ->
            if (ok) availableDataSender.onNext(data)
        }
        .doOnError(availableDataSender::onError)
        .doOnComplete(availableDataSender::onComplete)
        .onErrorComplete()
        .subscribe()
    handlerInfoSender
        .withLatestFrom(onAvailable) { handler, ok ->
            if (ok) availableHandlerSender.onNext(handler)
        }
        .onErrorComplete()
        .subscribe()
    return object :RemoteHandler{
        override val info: HandlerInfo
            get() = handlerInfo
        override val dataSender: Subject<ByteString>
            get() = dataSender
        override val onYieldedData: Observable<ByteString>
            get() = onYieldedData
        override val onRemoteHandler: Observable<RemoteHandler>
            get() = onRemoteHandler
        override val theEnd: Completable
            get() = onEnd.ignoreElements()

        override fun sendHandlerInfo(info: HandlerInfo) = handlerInfoSender.onNext(info)

    }
}