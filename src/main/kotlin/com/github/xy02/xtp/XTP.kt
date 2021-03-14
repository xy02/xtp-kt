package com.github.xy02.xtp

import com.github.xy02.rx.getSubValues
import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.*
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.PublishSubject
import xtp.*
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

//Socket，可表示任意有序的基础传输协议的Socket，TCP, WebSocket, QUIC等
class Socket(
    val onFrame: Observable<Frame>,
    val frameSender: Observer<Frame>,
)

//连接，使用init可以把Socket类型转换为Connection
class Connection(
    val flow: Flow,
    val channel: Channel,
)

//流的发送通道（流的生产者）
class Channel(
    //流消费者拉取消息的增量
    val onPull: Observable<Int>,
    //是否可发送流消息
    val onAvailable: Observable<Boolean>,
    //创建“子流”的发送通道，订阅后会发送“流头”消息
    val createChildChannel: (header: Header.Builder) -> Single<Channel>,
    //发送二进制数据
    val sendData: (ByteArray) -> Unit,
    //发送成功结束
    val sendComplete: () -> Unit,
    //发送错误结束
    val sendError: (Throwable) -> Unit,
    //二进制数据发送器
    val dataSender: Observer<ByteArray>,
)

//接收流（流的消费者）
class Flow(
    //流头，对端发来的流头
    val header: Header,
    //消息流的拉取器，收到的消息总数不得大于拉取的总和
    val messagePuller: Observer<Int>,
    //对端发来的消息流
    val onMessage: Observable<Message>,
    //对端发来的二进制数据流
    val onData: Observable<ByteArray>,
    //对端发来的“子流”（当收到“流头”消息时）
    val onChildFlow: Observable<Flow>,
    //按fn获取“子流”
    val getChildFlowByFn: (String) -> Observable<Flow>,
    //按fn获取一个“子流”
    val getSingleChildFlowByFn: (String) -> Single<Flow>,
    //自动流量控制，用于有多个下游通道时自动向上游拉取消息
    val pipeChannels: (PipeMap) -> Completable,
)

//pipe配置
class PipeSetup(
    //忽略的消息，会用于向上游的pull计数
    //收到的上游“流消息”与向下游发送的“已处理消息”必须1对1数量相等
    //“已处理消息”包括向下游发送的消息和忽略的消息
    val onIgnoredMessage: Observable<Int> = Observable.empty(),
)

//通道与向此通道发送的消息流的映射关系
typealias PipeMap = Map<Channel, PipeSetup>

class RemoteError(
    val type: String,
    override val message: String,
) : Throwable()

class ProtocolError(
    override val message: String = "",
) : Throwable()

internal data class WindowState(
    val windowSize: Int,
    val increment: Int,
    val decrement: Int
)

internal class Context(
    val socket: Socket,
    val newFlowId: () -> Int,
    val watchMessageFramesByFlowId: (Int) -> Observable<Frame>,
    val watchEndFramesByFlowId: (Int) -> Observable<Frame>,
    val watchPullFramesByFlowId: (Int) -> Observable<Frame>,
    val watchCancelFramesByFlowId: (Int) -> Observable<Frame>,
)

//用于socket初始化的函数
fun init(myHeader: Header.Builder, socket: Socket): Single<Connection> {
    val getFramesByType = getSubValues(socket.onFrame) { frame -> frame.typeCase }
    val messageFrames = getFramesByType(Frame.TypeCase.MESSAGE)
    val endFrames = getFramesByType(Frame.TypeCase.END)
    val pullFrames = getFramesByType(Frame.TypeCase.PULL)
    val cancelFrames = getFramesByType(Frame.TypeCase.CANCEL)
    val fid = AtomicInteger(1)
    val ctx = Context(
        socket = socket,
        newFlowId = { fid.getAndIncrement() },
        watchMessageFramesByFlowId = getSubValues(messageFrames, Frame::getFlowId),
        watchEndFramesByFlowId = getSubValues(endFrames, Frame::getFlowId),
        watchPullFramesByFlowId = getSubValues(pullFrames, Frame::getFlowId),
        watchCancelFramesByFlowId = getSubValues(cancelFrames, Frame::getFlowId),
    )
    val firstRemoteHeader = ctx.watchMessageFramesByFlowId(0)
        .map { frame ->
            if (frame.message.typeCase != Message.TypeCase.HEADER)
                throw ProtocolError("require first header message")
            frame.message.header
        }
        .take(1).singleOrError()
    return createChannel(ctx, 0)(myHeader)
        .flatMap { channel ->
            firstRemoteHeader.map { remoteHeader ->
                Connection(
                    newFlow(ctx, remoteHeader),
                    channel,
                )
            }
        }
}

internal fun newFlow(
    ctx: Context,
    header: Header
): Flow {
    val flowId = header.flowId
    val theEnd = ctx.watchEndFramesByFlowId(flowId)
        .take(1)
        .doOnNext { frame ->
            val end = frame.end
            if (end.hasError())
                throw RemoteError(end.error.type ?: "", end.error.strMessage ?: "")
        }
    val messagePuller = PublishSubject.create<Int>()
    val onMessage = ctx.watchMessageFramesByFlowId(flowId)
        .map { frame -> frame.message }
        .takeUntil(theEnd)
        .takeUntil(messagePuller.ignoreElements().toObservable<Any>())
        .share()
    val pullIncrements = getPullIncrements(onMessage, messagePuller)
    val pullFrames = pullIncrements
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
        .doOnNext { ctx.socket.frameSender.onNext(it) }
    pullFrames.subscribe() //side effect
    val getMessagesByType = getSubValues(onMessage, Message::getTypeCase)
    val onData = getMessagesByType(Message.TypeCase.DATA).map { it.data.toByteArray() }
    val onChildFlow = getMessagesByType(Message.TypeCase.HEADER).map { newFlow(ctx, it.header) }
    val getChildFlowByFn = getSubValues(onChildFlow) { flow -> flow.header.fn }
    val getSingleChildFlowByFn = { fn: String -> getChildFlowByFn(fn).take(1).singleOrError() }
    return Flow(
        header = header,
        messagePuller = messagePuller,
        onMessage = onMessage,
        onData = onData,
        onChildFlow = onChildFlow,
        getChildFlowByFn = getChildFlowByFn,
        getSingleChildFlowByFn = getSingleChildFlowByFn,
        pipeChannels = pipeChannels(messagePuller),
    )
}

internal fun createChannel(
    ctx: Context,
    parentFlowId: Int,
): (header: Header.Builder) -> Single<Channel> = { header ->
    Single.create<Channel> { emitter ->
        val flowId = ctx.newFlowId()
        val availableMessageSender = PublishSubject.create<Message>()
        val remoteCancel = ctx.watchCancelFramesByFlowId(flowId)
            .take(1)
            .doOnNext { frame ->
                val e = frame.cancel
                throw RemoteError(e.type, e.strMessage)
            }
        val sentItemsWithNoError = availableMessageSender
            .takeUntil(remoteCancel)
            .map { message -> Frame.newBuilder().setMessage(message) }
            .concatWith(Single.just(Frame.newBuilder().setEnd(End.getDefaultInstance())))
            .onErrorReturn { e ->
                val error = Error.newBuilder().setType(e.javaClass.name).setStrMessage(e.message ?: "")
                val end = End.newBuilder().setError(error)
                Frame.newBuilder().setEnd(end)
            }
            .doOnNext { fb ->
                val frame = fb.setFlowId(flowId).build()
                ctx.socket.frameSender.onNext(frame)
            }
            .share()
        val theEnd = sentItemsWithNoError.ignoreElements().toObservable<ByteArray>()
        val pulls = ctx.watchPullFramesByFlowId(flowId)
            .map { it.pull }
            .takeUntil(theEnd)
            .replay(1)
            .autoConnect()
        val availableAmount = Observable.merge(sentItemsWithNoError.map { -1 }, pulls)
            .scan(0, { a, b -> a + b })
        val isAvailable = availableAmount.map { amount -> amount > 0 }
            .distinctUntilChanged()
            .replay(1)
            .autoConnect()
        val dataSender = PublishSubject.create<ByteArray>()
        //side effect
        dataSender.withLatestFrom(isAvailable,
            { data, ok ->
                //不可发送的消息直接丢弃
                if (ok) {
                    val message = Message.newBuilder().setData(ByteString.copyFrom(data)).build()
                    availableMessageSender.onNext(message)
                }
            })
            .subscribe(
                { },
                availableMessageSender::onError,
                availableMessageSender::onComplete
            )
        val channel = Channel(
            onPull = pulls,
            onAvailable = isAvailable,
            createChildChannel = createChannel(ctx, flowId),
            sendData = dataSender::onNext,
            sendComplete = dataSender::onComplete,
            sendError = dataSender::onError,
            dataSender = dataSender,
        )
        emitter.onSuccess(channel)
        //发送header
        val headerMessage = Message.newBuilder()
            .setHeader(header.setFlowId(flowId))
        val headerFrame = Frame.newBuilder()
            .setFlowId(parentFlowId)
            .setMessage(headerMessage)
        ctx.socket.frameSender.onNext(headerFrame.build())
    }.cache()
}

internal fun pipeChannels(
    messagePuller: Observer<Int>,
): (PipeMap) -> Completable {
    return { channelMap ->
        val list = channelMap.map { (channel, setup) ->
            val theEnd = channel.onPull.ignoreElements().toObservable<Int>()
            Observable.merge(
                channel.onPull,
                setup.onIgnoredMessage.takeUntil(theEnd)
            ).doOnComplete { throw Exception() }
                .onErrorReturnItem(-1)
                .map { pull -> Pair(channel, pull) }
        }
        val stateMap = channelMap.mapValues { 0 }.toMutableMap()
        Observable.fromIterable(list)
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
}

internal fun getPullIncrements(
    messages: Observable<Message>,
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
