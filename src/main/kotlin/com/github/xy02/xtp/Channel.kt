package com.github.xy02.xtp

import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.subjects.PublishSubject
import xtp.*
import java.util.concurrent.ConcurrentLinkedQueue

//流消息的发送通道
class Channel internal constructor(
    //隶属的请求者
    val requester: Requester,
    //发送的响应
    val response: Response,
) {
    val flowId = requester.request.flowId
    private val conn = requester.conn
    private val availableMessageSender = PublishSubject.create<ByteArray>()
    private val remoteCancel = conn.watchCancelFrames(flowId)
        .take(1)
        .doOnNext { frame ->
            val e = frame.cancel
            throw RemoteError(e.type, e.strMessage)
        }
    private val sentItemsWithNoError = availableMessageSender
        .takeUntil(remoteCancel)
        .map { message -> Frame.newBuilder().setMessage(ByteString.copyFrom(message)) }
        .concatWith(Single.just(Frame.newBuilder().setEnd(End.getDefaultInstance())))
        .onErrorReturn { e ->
            val error = Error.newBuilder().setType(e.javaClass.name).setStrMessage(e.message ?: "")
            val end = End.newBuilder().setError(error)
            Frame.newBuilder().setEnd(end)
        }
        .doOnNext { fb ->
            val frame = fb.setFlowId(flowId).build()
            conn.frameSender.onNext(frame)
        }
        .share()
    private val theEnd = sentItemsWithNoError.ignoreElements().toObservable<ByteArray>()

    //收到拉取量
    val onPull: Observable<Int> = conn.watchPullFrames(flowId)
        .map { it.pull }
        .takeUntil(theEnd)
        .replay(1)
        .autoConnect()
    private val availableAmount = Observable.merge(sentItemsWithNoError.map { -1 }, onPull)
        .scan(0, { a, b -> a + b })

    //是否可发送流消息
    val onAvailable: Observable<Boolean> = availableAmount.map { amount -> amount > 0 }
        .distinctUntilChanged()
        .replay(1)
        .autoConnect()

    //流消息的发送器
    val messageSender: PublishSubject<ByteArray> = PublishSubject.create()

    private val queueSubject = PublishSubject.create<ByteArray>()
    private val queue = ConcurrentLinkedQueue<ByteArray>()

    //发送可缓存的数据
    val sendBuffer = queueSubject::onNext

    init {
        //side effect
        onPull
            .flatMap { pull ->
                Observable.generate<ByteArray> { emitter ->
                    val v = queue.poll()
                    if (v != null) emitter.onNext(v)
                    else emitter.onComplete()
                }.take(pull.toLong())
            }
            .doOnNext(availableMessageSender::onNext)
            .onErrorComplete()
            .subscribe()
        queueSubject.withLatestFrom(onAvailable,
            { data, ok ->
                if (ok) availableMessageSender.onNext(data)
                else queue.add(data)
            })
            .takeUntil(theEnd)
            .onErrorComplete()
            .subscribe()
        messageSender.withLatestFrom(onAvailable,
            { data, ok ->
                //不可发送的消息直接丢弃
                if (ok) availableMessageSender.onNext(data)
            })
            .takeUntil(theEnd)
            .subscribe(
                { },
                availableMessageSender::onError,
                availableMessageSender::onComplete
            )
        //发送response
        val frame = Frame.newBuilder()
            .setFlowId(flowId)
            .setResponse(response)
        conn.frameSender.onNext(frame.build())
    }

    //发送请求，订阅后会发送以“Request”序列化的消息(如果可以发送)
    fun sendRequest(req: Request.Builder, buffer: Boolean = false): Single<Responder> {
        val flowId = conn.newFlowId()
        val request = req.setFlowId(flowId).build()
        return onAvailable.take(1)
            .flatMap { ok ->
                val message = request.toByteArray()
                if (ok) {
                    //发送request
                    availableMessageSender.onNext(message)
                } else if (buffer) {
//                    println("add queue")
                    queue.add(message)
                } else {
                    throw ProtocolError("channel is not available")
                }
                conn.watchResponseFrames(flowId)
            }
            .take(1).singleOrError()
            .map { frame ->
                Responder(conn, request, frame.response)
            }
    }

}