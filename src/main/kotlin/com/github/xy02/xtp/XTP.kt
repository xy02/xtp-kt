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
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

data class Socket(
    val onBytes: Observable<ByteArray>,
    val bytesSender: Observer<ByteArray>,
)

data class InfoHeader(
    val peerInfo: PeerInfo,
    val register: Map<String, Accept> = mapOf(),
)

data class Connection(
    val remoteInfo: InfoHeader,
    val getStreamByType: (String) -> Single<Stream>,
    //创建流
    val createChannel: (header: Header.Builder) -> Channel,
    val close: (Throwable) -> Unit,
)

data class Channel(
    val onPull: Observable<Int>,
    val onAvailable: Observable<Boolean>,
    val messageSender: Observer<ByteArray>,
    val getStreamByType: (String) -> Single<Stream>,
)

data class Stream(
    val header: Header,
    val onMessage: Observable<ByteArray>,
    val messagePuller: Observer<Int>,
    val createChannel: (header: Header.Builder) -> Channel,
    val pipeChannels: (PipeConfig) -> UnPipe,
)

data class PipeConfig(
    val channelMap: Map<Channel, Observable<ByteArray>>,
    val pullIncrement: Int = 10000,
)

data class UnPipeError(
    override val message: String = "",
) : Throwable()

data class RemoteError(
    val type: String,
    override val message: String,
) : Throwable()

data class ProtocolError(
    override val message: String = "",
) : Throwable()

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
    val headerMap = register.mapValues { (streamName, accept) ->
        val subject = BehaviorSubject.create<Frame>()
        m[streamName] = Pair(subject, accept)
        subject
    }
    downstreamHeaderFrames
        .doOnNext { frame ->
            val streamName = frame.header.streamName
            val (emitter, accept) = m[streamName] ?: throw ProtocolError("not acceptable stream type")
            m.remove(streamName)
            emitter.onNext(frame)
//            emitter.onComplete()
        }
        .take(register.size.toLong())
        .subscribe(
            {},
            { e ->
                m.forEach { (streamName, pair) ->
                    val (emitter, accept) = pair
                    m.remove(streamName)
                    emitter.onError(e)
                }
                ctx.close(e)
            },
        )
    return { streamName ->
        val headerFrames = headerMap[streamName]?.take(1)?.singleOrError()
            ?: Single.error(ProtocolError("not acceptable stream type"))
        headerFrames.map { headerFrame -> createStream(ctx, headerFrame) }
    }
}

internal fun createStream(
    ctx: Context,
    headerFrame: Frame
): Stream {
    val streamId = headerFrame.header.streamId
    val header = headerFrame.header
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
        pipeChannels = pipeChannels(onMessage, dataPuller),
    )
}

internal fun createDownstreamChannel(
    ctx: Context,
    upstreamId: Int,
    remoteRegister: Map<String, Accept>,
    queueSize: Int = 1000,
): (header: Header.Builder) -> Channel = { header ->
    val dataSender = PublishSubject.create<ByteArray>()
    //缺少验证accept
    if (!remoteRegister.containsKey(header.streamName)) {
        val e = ProtocolError("the stream type is not accepted")
        Channel(
            onPull = Observable.error(e),
            onAvailable = Observable.error(e),
            messageSender = dataSender,
            getStreamByType = { Single.error(e) },
        )
    } else {
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
        val sumOfSendAndPull = Observable.merge(sentItemsWithNoError.map { -1 }, pulls)
            .scan(0, { a, b -> a + b })
        val isAvailable = sumOfSendAndPull.map { amount -> amount > 0 }
            .distinctUntilChanged()
            .replay(1)
            .autoConnect()
        val queue = ConcurrentLinkedQueue<ByteArray>()
        val dataFromQueue = pulls
            .flatMap { pull ->
                Observable
                    .generate<ByteArray> { emitter ->
                        val v = queue.poll()
                        if (v != null) emitter.onNext(v)
                        else emitter.onComplete()
                    }
                    .take(pull.toLong())
            }
        //side effect
        Observable.merge(
            dataFromQueue,
            dataSender.withLatestFrom(isAvailable,
                { data, ok ->
                    if (ok) Observable.just(data)
                    else Observable.empty<ByteArray>().doOnComplete {
                        if (queueSize > 0) {
                            if (queue.size == queueSize)
                                queue.poll()
                            queue.add(data)
                        }
                    }
                })
                .flatMap { it }
        )
            .doOnSubscribe {
                //send header，frame的stream_id是0
                val frame = Frame.newBuilder()
                    .setHeader(header.setStreamId(streamId).setUpstreamId(upstreamId))
                ctx.sendFrame(frame.build())
            }
            .subscribe(
                { data -> availableDataSender.onNext(data) },
                availableDataSender::onError,
                availableDataSender::onComplete
            )
        val downstreamHeaderFrames = ctx.getHeaderFramesByUpstreamId(streamId)
        val getStreamByType = getStream(ctx, header.registerMap, downstreamHeaderFrames)
        Channel(
            onPull = pulls,
            onAvailable = isAvailable,
            messageSender = dataSender,
            getStreamByType = getStreamByType,
        )
    }
}

internal fun pipeChannels(
    onData: Observable<ByteArray>,
    dataPuller: Observer<Int>,
): (PipeConfig) -> UnPipe {
    return { (channelMap, pullIncrement) ->
        val clearMap = channelMap.mapValues { (channel, outputData) ->
            val dataSender = channel.messageSender
            //向下游输出处理过的数据
            val disposable = outputData.subscribe(dataSender::onNext, dataSender::onError, dataSender::onComplete)
            return@mapValues {
                disposable.dispose()
                dataSender.onError(UnPipeError())
            }
        }
        val availableList = channelMap.map { (channel, _) -> channel.onAvailable }
        val onAllAvailable = Observable.combineLatest(availableList) {
            it.fold(true, { pre, available -> pre && available as Boolean })
        }
        var pendingPull = pullIncrement
        val d = onData.map { 1 }
            .withLatestFrom(onAllAvailable.doOnNext { ok ->
                if (ok) {
                    val pull = pendingPull
                    pendingPull = 0
                    //向上游拉取
                    dataPuller.onNext(pull)
                }
            }, { pull, ok ->
                if (ok) dataPuller.onNext(pull) else pendingPull += pull
            })
            .onErrorComplete()
            .subscribe()
        ;
        {
            d.dispose()
            clearMap.forEach { (_, clear) -> clear() }
            dataPuller.onError(UnPipeError())
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
