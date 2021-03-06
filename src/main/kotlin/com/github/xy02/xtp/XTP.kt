package com.github.xy02.xtp

import com.github.xy02.rx.getSubValues
import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.*
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.BehaviorSubject
import io.reactivex.rxjava3.subjects.PublishSubject
import xtp.*
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

//Socket，可表示任意有序的基础传输协议的Socket，TCP, WebSocket, QUIC等
data class Socket(
    val onBytes: Observable<ByteArray>,
    val bytesSender: Observer<ByteArray>,
)

//信息头，用于连接首次发送流头
data class InfoHeader(
    //端信息
    val peerInfo: PeerInfo,
    //消息类型的注册，表示允许接收的下游流消息类型
    val register: Map<String, Accept> = mapOf(),
)

//连接，使用initWith可以把Socket类型转换为Connection
data class Connection(
    //对端信息
    val remoteInfo: InfoHeader,
    //获取对端发来的下游流
    val getStreamByType: (String) -> Single<Stream>,
    //创建下游流的发送通道，订阅后会发送“流头”消息
    val createChannel: (header: Header.Builder) -> Single<Channel>,
    //关闭连接
    val close: (Throwable) -> Unit,
)

//流的发送通道
data class Channel(
    //流消费者拉取消息的增量
    val onPull: Observable<Int>,
    //是否可发送流消息
    val onAvailable: Observable<Boolean>,
    //流消息的发送器
    val messageSender: Observer<ByteArray>,
    //获取对端发来的下游流
    val getStreamByType: (String) -> Single<Stream>,
    //创建“子流”的发送通道，订阅后会发送“流头”消息
    val createChildChannel: (header: Header.Builder) -> Single<Channel>,
)

//接收流
data class Stream(
    //流头
    val header: Header,
    //接收到的消息流
    val onMessage: Observable<ByteArray>,
    //消息的拉取器
    val messagePuller: Observer<Int>,
    //创建下游流的发送通道，订阅后会发送“流头”消息
    val createChannel: (header: Header.Builder) -> Single<Channel>,
    //把接收的消息以自动流量控制的方式排向多个发送通道
    val pipeChannels: (PipeMap) -> UnPipe,
    //接收发来的“子流”
    val getChildStreams: () -> Observable<Stream>,
)
//通道与向此通道发送的消息流的映射关系
typealias PipeMap = Map<Single<Channel>, Observable<HandledMessage>>

data class RemoteError(
    val type: String,
    override val message: String,
) : Throwable()

data class ProtocolError(
    override val message: String = "",
) : Throwable()

//已处理消息，用于自动流量控制时发送给通道，收到的上游“流消息”与向下游发送的“已处理消息”必须1对1数量相等
class HandledMessage(
    //消息数据
    val message: ByteArray,
    //忽略此消息，实际不会发送给通道，但会用于向上游的pull计数
    val ignore: Boolean = false,
)

//取消pipeChannels
typealias UnPipe = () -> Unit


internal data class WindowState(
    val windowSize: Int,
    val increment: Int,
    val decrement: Int
)

internal data class Context(
    val newStreamId: () -> Int,
    val sendFrame: (Frame) -> Unit,
    val close: (Throwable) -> Unit,
    val getCancelFramesByStreamId: (Int) -> Observable<Frame>,
    val getPullFramesByStreamId: (Int) -> Observable<Frame>,
    val getHeaderFramesByUpstreamId: (Int) -> Observable<Frame>,
    val getEndFramesByStreamId: (Int) -> Observable<Frame>,
    val getMessageFramesByStreamId: (Int) -> Observable<Frame>,
)

//用于socket初始化的函数
fun initWith(myInfo: InfoHeader): (Socket) -> Single<Connection> {
    return initWith(Single.just(myInfo))
}

//用于socket初始化的函数
fun initWith(onMyInfo: Single<InfoHeader>): (Socket) -> Single<Connection> {
    val initialSid = 1
    val singleMyInfoPair = onMyInfo.map { myInfo ->
        val header = Header.newBuilder()
            .setStreamId(initialSid)
            .setUpstreamId(0)
            .putAllRegister(myInfo.register)
            .setInfo(myInfo.peerInfo.toByteString())
        val bytes = Frame.newBuilder().setStreamId(0).setHeader(header).build().toByteArray()
        Pair(bytes, myInfo)
    }.cache()
    return { socket ->
        singleMyInfoPair.flatMap { (myInfoBytes, myInfo) ->
            val frames = socket.onBytes.map(Frame::parseFrom)
//                .doOnNext { frame->
//                    println("receive frame: $frame")
//                }
                .share()
            val getFramesByType = getSubValues(frames) { frame -> frame.typeCase }
            val headerFrames = getFramesByType(Frame.TypeCase.HEADER)
            val messageFrames = getFramesByType(Frame.TypeCase.MESSAGE)
            val endFrames = getFramesByType(Frame.TypeCase.END)
            val pullFrames = getFramesByType(Frame.TypeCase.PULL)
            val cancelFrames = getFramesByType(Frame.TypeCase.CANCEL)
            val close = { err: Throwable ->
                //发送错误
                val end = End.newBuilder().setError(
                    Error.newBuilder()
                        .setType(err.javaClass.name)
                        .setStrMessage(err.message)
                )
                val frame = Frame.newBuilder().setStreamId(0).setEnd(end).build()
                socket.bytesSender.onNext(frame.toByteArray())
                //关闭连接
                socket.bytesSender.onComplete()
            }
            val sid = AtomicInteger(initialSid + 1)
            val ctx = Context(
                newStreamId = { sid.getAndIncrement() },
                sendFrame = { frame: Frame -> socket.bytesSender.onNext(frame.toByteArray()) },
                close = close,
                getCancelFramesByStreamId = getSubValues(cancelFrames, Frame::getStreamId),
                getPullFramesByStreamId = getSubValues(pullFrames, Frame::getStreamId),
                getHeaderFramesByUpstreamId = getSubValues(headerFrames) { frame -> frame.header.upstreamId },
                getMessageFramesByStreamId = getSubValues(messageFrames, Frame::getStreamId),
                getEndFramesByStreamId = getSubValues(endFrames, Frame::getStreamId),
            )
            //缺少下游流头结束条件
            val downstreamHeaderFrames = ctx.getHeaderFramesByUpstreamId(initialSid)
            val getStreamByType = getStream(ctx, myInfo.register, downstreamHeaderFrames)
            val singleRemoteInfo = headerFrames
                .map { frame ->
                    val peerInfo = PeerInfo.parseFrom(frame.header.info)
                    InfoHeader(peerInfo, frame.header.registerMap)
                }.take(1).singleOrError()
            singleRemoteInfo
                .doOnSubscribe {
                    //send myInfo
                    socket.bytesSender.onNext(myInfoBytes)
                }
                .map { remoteInfo ->
                    Connection(
                        remoteInfo = remoteInfo, getStreamByType = getStreamByType, close = close,
                        createChannel = createDownstreamChannel(ctx, initialSid, remoteInfo.register),
                    )
                }
        }
    }
}

internal fun getStream(
    ctx: Context,
    register: Map<String, Accept>,
    downstreamHeaderFrames: Observable<Frame>,
): (String) -> Single<Stream> {
    val m = ConcurrentHashMap<String, Pair<BehaviorSubject<Frame>, Accept>>()
    val headerMap = register.mapValues { (messageType, accept) ->
        val subject = BehaviorSubject.create<Frame>()
        m[messageType] = Pair(subject, accept)
        subject
    }
    downstreamHeaderFrames
        .doOnNext { frame ->
            val messageType = frame.header.messageType
            val (emitter, accept) = m[messageType] ?: throw ProtocolError("not acceptable stream type")
            m.remove(messageType)
            emitter.onNext(frame)
//            emitter.onComplete()
        }
        .take(register.size.toLong())
        .subscribe(
            {},
            { e ->
                m.forEach { (messageType, pair) ->
                    val (emitter, accept) = pair
                    m.remove(messageType)
                    emitter.onError(e)
                }
                ctx.close(e)
            },
        )
    return { messageType ->
        val headerFrames = headerMap[messageType]?.take(1)?.singleOrError()
            ?: Single.error(ProtocolError("not acceptable stream type"))
        headerFrames.map { headerFrame -> createStream(ctx, headerFrame.header) }
    }
}

internal fun createStream(
    ctx: Context,
    header: Header
): Stream {
    val streamId = header.streamId
    val theEnd = ctx.getEndFramesByStreamId(streamId)
        .take(1)
        .doOnNext { frame ->
            val end = frame.end
            if (end.hasError())
                throw RemoteError(end.error.type, end.error.strMessage)
        }
    val dataPuller = PublishSubject.create<Int>()
    val onMessage = ctx.getMessageFramesByStreamId(streamId)
        .map { frame -> frame.message.toByteArray() }
        .takeUntil(theEnd)
        .takeUntil(dataPuller.ignoreElements().toObservable<Any>())
        .share()
    val pullIncrements = getPullIncrements(
        onMessage, dataPuller, ProtocolError("input buf overflow")
    )
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
        .map { builder -> builder.setStreamId(streamId).build() }
        .doOnNext { ctx.sendFrame(it) }
    pullFrames.subscribe() //side effect
    return Stream(
        header = header,
        onMessage = onMessage,
        messagePuller = dataPuller,
        createChannel = createDownstreamChannel(ctx, streamId, header.registerMap),
        pipeChannels = pipeChannels(dataPuller),
        getChildStreams = parseHeaderStream(ctx, onMessage)
    )
}

//解析消息为“流头”的流
internal fun parseHeaderStream(ctx: Context, onMessage: Observable<ByteArray>): () -> Observable<Stream> {
    val streams = onMessage.map(Header::parseFrom)
        .map { header -> createStream(ctx, header) }
        .share()
    return {
        streams
    }
}

internal fun createDownstreamChannel(
    ctx: Context,
    upstreamId: Int,
    remoteRegister: Map<String, Accept>,
): (header: Header.Builder) -> Single<Channel> {
    val newChannel = createChannel(ctx, 0, upstreamId)
    return { header ->
        if (!remoteRegister.containsKey(header.messageType)) {
            //缺少验证accept
            Single.error(ProtocolError("the messageType is not accepted"))
        } else {
            newChannel(header)
        }
    }
}

internal fun createChannel(
    ctx: Context,
    parentStreamId: Int,
    upstreamId: Int,
): (header: Header.Builder) -> Single<Channel> = { header ->
    Single.create<Channel> { emitter ->
        val streamId = ctx.newStreamId()
        val availableDataSender = PublishSubject.create<ByteArray>()
        val remoteCancel = ctx.getCancelFramesByStreamId(streamId)
            .take(1)
            .doOnNext { frame ->
                val e = frame.cancel
                throw RemoteError(e.type, e.strMessage)
            }
        val sentItemsWithNoError = availableDataSender
            .takeUntil(remoteCancel)
            .doOnNext { data ->
                val frame = Frame.newBuilder().setStreamId(streamId).setMessage(ByteString.copyFrom(data))
                ctx.sendFrame(frame.build())
            }
            .doOnError { e ->
                val error = Error.newBuilder().setType(e.javaClass.name).setStrMessage(e.message)
                val end = End.newBuilder().setError(error)
                val frame = Frame.newBuilder().setStreamId(streamId).setEnd(end)
                ctx.sendFrame(frame.build())
            }
            .doOnComplete {
                val frame = Frame.newBuilder().setStreamId(streamId).setEnd(End.getDefaultInstance())
                ctx.sendFrame(frame.build())
            }
            .onErrorComplete()
            .share()
        val theEnd = sentItemsWithNoError.ignoreElements().toObservable<ByteArray>()
        val pulls = ctx.getPullFramesByStreamId(streamId)
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
        val messageSender = PublishSubject.create<ByteArray>()
        val downstreamHeaderFrames = ctx.getHeaderFramesByUpstreamId(streamId)
        val getStreamByType = getStream(ctx, header.registerMap, downstreamHeaderFrames)
        val channel = Channel(
            onPull = pulls,
            onAvailable = isAvailable,
            messageSender = messageSender,
            getStreamByType = getStreamByType,
            createChildChannel = createChannel(ctx, streamId, 0)
        )
        emitter.onSuccess(channel)
        //side effect
        messageSender.withLatestFrom(isAvailable,
            { data, ok ->
                //不可发送的消息直接丢弃
                if (ok) availableDataSender.onNext(data)
            })
            .subscribe(
                { },
                availableDataSender::onError,
                availableDataSender::onComplete
            )
        //发送header
        val frame = Frame.newBuilder()
            .setStreamId(parentStreamId)
            .setHeader(header.setStreamId(streamId).setUpstreamId(upstreamId))
        ctx.sendFrame(frame.build())
    }.cache()
}

internal fun pipeChannels(
    messagePuller: Observer<Int>,
): (PipeMap) -> UnPipe {
    return { channelMap ->
        val list = channelMap.map { (singleChannel, handledMessages) ->
            singleChannel.flatMapObservable { channel ->
                val dataSender = channel.messageSender
                val theEnd = channel.onPull.ignoreElements().toObservable<Int>()
                Observable.merge(
                    channel.onPull,
                    handledMessages
                        .takeUntil(theEnd)
                        .doOnNext {
                            //向下游输出处理过的数据
                            if (!it.ignore) dataSender.onNext(it.message)
                        }
                        .doOnError(dataSender::onError)
                        .doOnComplete(dataSender::onComplete)
                        .filter { it.ignore }
                        .map { 1 }
                ).doOnComplete { throw Exception() }
                    .onErrorReturnItem(-1)
                    .map { pull -> Pair(singleChannel, pull) }
            }
        }
        val stateMap = channelMap.mapValues { 0 }.toMutableMap()
        val d = Observable.fromIterable(list)
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
            .subscribe()
        ;
        {
            d.dispose()
        }
    }
}

internal fun getPullIncrements(
    messages: Observable<ByteArray>,
    pulls: Observable<Int>,
    overflowErr: Throwable,
): Observable<Int> = Observable.create { emitter ->
    val sub = Observable.merge(
        messages.map { -1 }, pulls
    ).scan(
        WindowState(0, 0, 0),
        { preState, num ->
            var (windowSize, increment, decrement) = preState
            if (num > 0) increment += num else decrement -= num
            if (decrement > windowSize) throw overflowErr
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
