package com.github.xy02.xtp

import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.PublishSubject
import xtp.Error
import xtp.Frame
import xtp.Request

//响应的消息流
class Flow internal constructor(
    //隶属的请求器
    private val requester: Requester,
) {
    private val flowId = requester.request.flowId
    private val conn = requester.conn
    private val frameSender = conn.frameSender
    private val theEnd = conn.watchEndFrames(flowId)
        .take(1)
        .doOnNext { frame ->
            val end = frame.end
            if (end.hasError())
                throw RemoteError(end.error.type ?: "", end.error.strMessage ?: "")
        }

    //流消息的拉取器
    val messagePuller = PublishSubject.create<Int>()

    //收到流消息
    val onMessage = conn.watchMessageFrames(flowId)
        .map { frame -> frame.message.toByteArray() }
        .takeUntil(theEnd)
        .takeUntil(messagePuller.ignoreElements().toObservable<Any>())
        .share()
    private val onPullIncrement = getPullIncrements(onMessage, messagePuller)
    private val onPullFrame = onPullIncrement
        .map { pull -> Frame.newBuilder().setPull(pull) }
        .doOnComplete { throw Exception("cancel") }
        .onErrorReturn { err ->
            val type = err.javaClass.name
            val message = err.message ?: ""
            Frame.newBuilder().setCancel(
                Error.newBuilder().setType(type).setStrMessage(message)
            )
        }
        .map { builder -> builder.setFlowId(flowId).build() }
        .doOnNext { frameSender.onNext(it) }

    //接收到"Request"消息时新建的响应器
    val onResponder: Observable<Responder> = onMessage.map(Request::parseFrom)
        .doOnNext { req ->
            if (req.flowId <= 0)
                throw ProtocolError("Request.flowId must greater than 0")
//            if (req.type.isNullOrEmpty())
//                throw ProtocolError("Request.type can not be empty")
        }
        .map { Responder(requester.conn, it) }
        .share()

    init {
        onPullFrame.subscribe() //side effect
    }

    //拉取消息
    fun pull(number: Int) {
        messagePuller.onNext(number)
    }

    data class WindowState(
        val windowSize: Int,
        val increment: Int,
        val decrement: Int
    )

    private fun getPullIncrements(
        messages: Observable<ByteArray>,
        pulls: Observable<Int>,
    ): Observable<Int> = Observable.create { emitter ->
        val sub = Observable.merge(
            messages.map { -1 }, pulls
        ).scan(
            WindowState(0, 0, 0),
            { preState, num ->
                var (windowSize, increment, decrement) = preState
                if (num > 0) increment += num else decrement -= num
                if (decrement > windowSize) throw ProtocolError("input overflow")
                if (decrement >= windowSize / 2) {
                    windowSize = windowSize - decrement + increment
                    if (increment > 0) emitter.onNext(increment)
                    increment = 0
                    decrement = 0
                }
                WindowState(windowSize, increment, decrement)
            }
        ).subscribe({ }, emitter::tryOnError, emitter::onComplete)
        emitter.setDisposable(Disposable.fromAction { sub.dispose() })
    }
}