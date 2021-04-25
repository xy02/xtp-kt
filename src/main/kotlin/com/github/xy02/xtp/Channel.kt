package com.github.xy02.xtp

import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.Maybe
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.subjects.PublishSubject
import io.reactivex.rxjava3.subjects.SingleSubject
import io.reactivex.rxjava3.subjects.Subject
import xtp.End
import xtp.Error
import xtp.Frame
import xtp.Header
import java.util.concurrent.ConcurrentLinkedQueue

//流消息的发送通道（消息的生产者）
data class Channel(
    //XTP连接
    val conn: Connection,
    //发送的流头
    val header: Header,
    //流消息的发送器
    val messageSender: Subject<ByteArray>,
    //远端取消流
    val onRemoteCancel: Maybe<Error>,
    //收到拉取量
    val onPull: Observable<Int>,
    //可发送的消息数量
    val onAvailableAmount: Observable<Int>,
    //是否可发送流消息
    val onAvailable: Observable<Boolean>,
    //发送流头消息
    val sendHeader: (headerBuilder: Header.Builder) -> Single<Channel>,
)

private data class HeaderToBeSent(
    val header: Header,
    val onChannel: SingleSubject<Channel>,
)

internal fun newChannel(ctx: Context, header: Header): Channel {
    val newFlowId = ctx.newFlowId
    val frameSender = ctx.conn.frameSender
    val watchCancelFrames = ctx.watchCancelFrames
    val watchPullFrames = ctx.watchPullFrames
    val flowId = header.flowId
    val availableMessageSender = PublishSubject.create<ByteArray>().toSerialized()
    val messageSender: Subject<ByteArray> = PublishSubject.create<ByteArray>().toSerialized()
    val endOfSending = messageSender
        .doOnComplete {
            val frame = Frame.newBuilder()
                .setFlowId(flowId).setEnd(End.getDefaultInstance()).build()
            frameSender.onNext(frame)
        }
        .doOnError { e ->
            val error = Error.newBuilder().setTypeName(e.javaClass.name).setTextMessage(e.message ?: "")
            val end = End.newBuilder().setError(error)
            val frame = Frame.newBuilder().setFlowId(flowId).setEnd(end).build()
            frameSender.onNext(frame)
        }
        .lastElement().toObservable().onErrorComplete()
    val onRemoteCancel = watchCancelFrames(flowId)
        .take(1)
        .takeUntil(endOfSending)
        .map { frame -> frame.cancel }
        .singleElement()
        .cache()
    val theEnd = onRemoteCancel.toObservable()
    val onMessageSent = availableMessageSender
        .map { message -> Frame.newBuilder().setMessage(ByteString.copyFrom(message)) }
        .doOnNext { fb ->
            val frame = fb.setFlowId(flowId).build()
            frameSender.onNext(frame)
        }
        .takeUntil(theEnd)
        .onErrorComplete()
        .share()
    val onPull: Observable<Int> = watchPullFrames(flowId)
        .map { it.pull }
        .takeUntil(theEnd)
        .replay(1)
        .autoConnect()
    val onAvailableAmount: Observable<Int> = Observable.merge(onMessageSent.map { -1 }, onPull)
        .scan(0) { a, b -> a + b }
        .replay(1)
        .autoConnect()
    val onAvailable: Observable<Boolean> = onAvailableAmount.map { amount -> amount > 0 }
        .distinctUntilChanged()
        .replay(1)
        .autoConnect()
    val messageQueue = ConcurrentLinkedQueue<ByteArray>()
    val queueOfHeaderToBeSent = ConcurrentLinkedQueue<HeaderToBeSent>()
    val messageFromQueue = onPull
        .flatMap { pull ->
            Observable.merge(
                Observable
                    .generate<ByteArray> { emitter ->
                        val v = messageQueue.poll()
                        if (v != null) emitter.onNext(v)
                        else emitter.onComplete()
                    },
                Observable
                    .generate { emitter ->
                        val v = queueOfHeaderToBeSent.poll()
                        if (v != null) {
                            val channel = newChannel(ctx, v.header)
                            v.onChannel.onSuccess(channel)
                            emitter.onNext(v.header.toByteArray())
                        } else emitter.onComplete()
                    }
            ).take(pull.toLong())
        }
    val headerSender = PublishSubject.create<HeaderToBeSent>().toSerialized()
    //side effect
    Observable.merge(
        messageFromQueue,
        messageSender
            .withLatestFrom(onAvailable) { message, ok ->
                if (ok) Observable.just(message)
                else {
                    messageQueue.add(message)
                    Observable.empty()
                }
            }
            .flatMap { it },
        headerSender
            .withLatestFrom(onAvailable) { headerToBeSent, ok ->
                if (ok) {
                    val header = headerToBeSent.header
                    val channel = newChannel(ctx, header)
                    headerToBeSent.onChannel.onSuccess(channel)
                    Maybe.just(header.toByteArray())
                } else {
                    //                        println("headerToBeSent$headerToBeSent")
                    queueOfHeaderToBeSent.add(headerToBeSent)
                    Maybe.empty()
                }
            }
            .flatMapMaybe { it }
    ).takeUntil(theEnd)
        .subscribe(availableMessageSender)
    return Channel(
        conn = ctx.conn,
        header = header,
        messageSender = messageSender,
        onRemoteCancel = onRemoteCancel,
        onPull = onPull,
        onAvailableAmount = onAvailableAmount,
        onAvailable = onAvailable,
        sendHeader = sendHeader(newFlowId, headerSender)
    )
}

private fun sendHeader(
    newFlowId: () -> Int,
    headerSender: Subject<HeaderToBeSent>,
): (headerBuilder: Header.Builder) -> Single<Channel> = { headerBuilder ->
    val onChannel = SingleSubject.create<Channel>()
    val header = headerBuilder.setFlowId(newFlowId()).build()
    headerSender.onNext(HeaderToBeSent(header, onChannel))
    onChannel
}