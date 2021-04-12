package com.github.xy02.xtp

import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Maybe
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.PublishSubject
import xtp.Error
import xtp.Frame
import xtp.Header

//通道与向此通道发送的消息流的映射关系
typealias PipeMap = Map<Channel, PipeSetup>
//pipe配置
class PipeSetup(
    //忽略的消息，会用于向上游的pull计数
    //收到的上游“流消息”与向下游发送的“已处理消息”必须1对1数量相等
    //“已处理消息”包括向下游发送的消息和忽略的消息
    val onIgnoredMessage: Observable<Int> = Observable.empty(),
)

//响应的消息流
class Flow internal constructor(
    //连接
    val conn: Connection,
    //收到的流头
    val header: Header,
    //父流
    val parentFlow: Flow? = null,
) {
    private val flowId = header.flowId
    private val frameSender = conn.frameSender
    //流消息的拉取器
    val messagePuller = PublishSubject.create<Int>().toSerialized()

    private val theEnd = conn.watchEndFrames(flowId)
        .take(1)
        .takeUntil(messagePuller.lastElement().toObservable())
        .doOnNext { frame ->
            val end = frame.end
            if (end.hasError())
                throw RemoteError(end.error.typeName ?: "", end.error.strMessage ?: "")
        }

    //收到流消息
    val onMessage = conn.watchMessageFrames(flowId)
        .map { frame -> frame.message.toByteArray() }
        .takeUntil(theEnd)
        .share()
    private val onPull = messagePuller
        .doOnComplete { throw Exception("cancel") }
        .doOnError { err ->
            val name = err.javaClass.name
            val message = err.message ?: ""
            val frame = Frame.newBuilder().setCancel(
                Error.newBuilder().setTypeName(name).setStrMessage(message)
            ).setFlowId(flowId).build()
            frameSender.onNext(frame)
        }
        .takeUntil(theEnd)
    private val onPullIncrement = getPullIncrements(onMessage, onPull)
    private val onPullFrame = onPullIncrement
        .map { pull -> Frame.newBuilder().setPull(pull).setFlowId(flowId).build() }
        .doOnNext { frameSender.onNext(it) }
        .onErrorComplete()

    //接收到子流
    val onChildFlow: Observable<Flow> = onMessage
        .flatMapMaybe {
            try {
                val header = Header.parseFrom(it)
                if (header.flowId <= 0)
                    throw ProtocolError("Header.flowId must greater than 0")
                if (header.infoType.isNullOrEmpty())
                    throw ProtocolError("Header.infoType can not be empty")
                Maybe.just(header)
            } catch (e: Exception) {
                //发送cancel
                messagePuller.onError(e)
                Maybe.empty()
            }
        }
        .map { Flow(conn, it, this) }
        .share()

    init {
        onPullFrame.subscribe() //side effect
    }

    //拉取消息
    fun pull(number: Int) {
        messagePuller.onNext(number)
    }

    //自动流量控制
    fun pipeChannels(channelMap:PipeMap) : Completable {
        val list = channelMap.map { (channel, setup) ->
            val theEnd = channel.onPull.lastElement().toObservable()
            Observable.merge(
                channel.onPull,
                setup.onIgnoredMessage.takeUntil(theEnd)
            ).doOnComplete { throw Exception() }
                .onErrorReturnItem(-1)
                .map { pull -> Pair(channel, pull) }
        }
        val stateMap = channelMap.mapValues { 0 }.toMutableMap()
        return Observable.fromIterable(list)
            .flatMap { it }
            .scan(stateMap, { state, (key, pull) ->
                if (pull == -1) {
                    state.remove(key)
                    return@scan state
                }
                state[key] = state[key]?.plus(pull) ?: 0
                //找最小的pull，并向上游拉取
                var minPull = Int.MAX_VALUE
                state.forEach { (_, remain) ->
                    if (remain < minPull)
                        minPull = remain
                }
                if (minPull != 0) {
                    messagePuller.onNext(minPull)
                    state.mapValues { (_, v) -> v - minPull }.toMutableMap()
                } else
                    state
            })
            .ignoreElements()
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