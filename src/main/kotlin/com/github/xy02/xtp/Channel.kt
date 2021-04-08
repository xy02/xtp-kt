package com.github.xy02.xtp

import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.Maybe
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.subjects.PublishSubject
import io.reactivex.rxjava3.subjects.SingleSubject
import xtp.End
import xtp.Error
import xtp.Frame
import xtp.Header
import java.util.concurrent.ConcurrentLinkedQueue

class HeaderToBeSent(
    val header: Header,
    val onChannel: SingleSubject<Channel>,
)

//流消息的发送通道
class Channel internal constructor(
    //连接
    val conn: Connection,
    //发送的流头
    val header: Header,
    //父通道
    val parentChannel: Channel? = null,
) {
    val flowId = header.flowId
    private val availableMessageSender = PublishSubject.create<ByteArray>().toSerialized()
    val onRemoteCancel = conn.watchCancelFrames(flowId)
        .take(1)
        .doOnNext { frame ->
            val e = frame.cancel
            throw RemoteError(e.typeName, e.strMessage)
        }
        .share()
    private val onMessageSent = availableMessageSender
        .map { message -> Frame.newBuilder().setMessage(ByteString.copyFrom(message)) }
        .concatWith(Single.just(Frame.newBuilder().setEnd(End.getDefaultInstance())))
        .onErrorReturn { e ->
            val error = Error.newBuilder().setTypeName(e.javaClass.name).setStrMessage(e.message ?: "")
            val end = End.newBuilder().setError(error)
            Frame.newBuilder().setEnd(end)
        }
        .doOnNext { fb ->
            val frame = fb.setFlowId(flowId).build()
            conn.frameSender.onNext(frame)
        }
        .takeUntil(onRemoteCancel)
        .onErrorComplete()
        .share()
    private val theEnd = onMessageSent.ignoreElements().toObservable<ByteArray>()

    //收到拉取量
    val onPull: Observable<Int> = conn.watchPullFrames(flowId)
        .map { it.pull }
        .takeUntil(theEnd)
        .replay(1)
        .autoConnect()

    private val onAvailableAmount = Observable.merge(onMessageSent.map { -1 }, onPull)
        .scan(0, { a, b -> a + b })

    //是否可发送流消息
    val onAvailable: Observable<Boolean> = onAvailableAmount.map { amount -> amount > 0 }
        .distinctUntilChanged()
        .replay(1)
        .autoConnect()

    //    private val queueSubject = PublishSubject.create<ByteArray>().toSerialized()
    private val messageQueue = ConcurrentLinkedQueue<ByteArray>()
    private val queueOfHeaderToBeSent = ConcurrentLinkedQueue<HeaderToBeSent>()
    private val messageFromQueue = onPull
        .flatMap { pull ->
            Observable.merge(
                Observable
                    .generate<ByteArray> { emitter ->
                        val v = messageQueue.poll()
                        if (v != null) emitter.onNext(v)
                        else emitter.onComplete()
                    },
                Observable
                    .generate<ByteArray> { emitter ->
                        val v = queueOfHeaderToBeSent.poll()
                        if (v != null) {
                            val channel = Channel(conn, v.header, this)
                            v.onChannel.onSuccess(channel)
                            emitter.onNext(v.header.toByteArray())
                        } else emitter.onComplete()
                    }
            ).take(pull.toLong())
        }

    private val onHeaderToBeSent = PublishSubject.create<HeaderToBeSent>().toSerialized()

    //流消息的发送器
    val messageSender = PublishSubject.create<ByteArray>().toSerialized()

    init {
        //side effect
        Observable.merge(
            messageFromQueue,
            messageSender.withLatestFrom(onAvailable,
                { message, ok ->
                    if (ok) Observable.just(message)
                    else {
                        messageQueue.add(message)
                        Observable.empty()
                    }
                })
                .flatMap { it },
            onHeaderToBeSent.withLatestFrom(onAvailable,
                { headerToBeSent, ok ->
                    if (ok) {
                        val header = headerToBeSent.header
                        val channel = Channel(conn, header, this)
                        headerToBeSent.onChannel.onSuccess(channel)
                        Maybe.just(header.toByteArray())
                    } else {
//                        println("headerToBeSent$headerToBeSent")
                        queueOfHeaderToBeSent.add(headerToBeSent)
                        Maybe.empty()
                    }
                })
                .flatMapMaybe { it }
        ).takeUntil(theEnd)
            .subscribe(availableMessageSender)
    }

    //发送流头
    fun sendHeader(headerBuilder: Header.Builder): Single<Channel> {
        val onChannel = SingleSubject.create<Channel>()
        val header = headerBuilder.setFlowId(conn.newFlowId()).build()
        onHeaderToBeSent.onNext(HeaderToBeSent(header, onChannel))
        return onChannel
    }

}

