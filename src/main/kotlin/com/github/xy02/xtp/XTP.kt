package com.github.xy02.xtp

import com.github.xy02.rx.getSubValues
import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.PublishSubject
import xtp.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

data class Socket(
    val onBytes: Observable<ByteArray>,
    val bytesSender: Observer<ByteArray>,
)

data class Connection(
    val remoteInfo: Info,
    val getStreamsByType: (String) -> Observable<Stream>,
    //创建流
    val createChannel: (header: Header.Builder) -> Channel,
)

data class Channel(
    val onPull: Observable<Int>,
    val onAvailable: Observable<Boolean>,
    val dataSender: Observer<ByteArray>,
    val getStreamsByType: (String) -> Observable<Stream>,
)

data class Stream(
    val header: Header,
    val onData: Observable<ByteArray>,
    val dataPuller: Observer<Int>,
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
    val getErrorFramesByStreamId: (Int) -> Observable<Frame>,
    val getPullFramesByStreamId: (Int) -> Observable<Frame>,
    val getHeaderFramesByParentStreamId: (Int) -> Observable<Frame>,
    val getEndFramesByStreamId: (Int) -> Observable<Frame>,
    val getMessageFramesByStreamId: (Int) -> Observable<Frame>,
)

fun init(
    socket: Socket,
    myInfo: Info,
): Single<Connection> {
    val frames = socket.onBytes.map(Frame::parseFrom).share()
    val getFramesByType = getSubValues(frames) { frame -> frame.typeCase }
    val headerFrames = getFramesByType(Frame.TypeCase.HEADER)
    val messageFrames = getFramesByType(Frame.TypeCase.MESSAGE)
    val endFrames = getFramesByType(Frame.TypeCase.END)
    val pullFrames = getFramesByType(Frame.TypeCase.PULL)
    val errorFrames = getFramesByType(Frame.TypeCase.ERROR)
    val sid = AtomicInteger()
    val ctx = Context(
        newStreamId = { sid.getAndIncrement() },
        sendFrame = { frame: Frame -> socket.bytesSender.onNext(frame.toByteArray()) },
        getErrorFramesByStreamId = getSubValues(errorFrames, Frame::getStreamId),
        getPullFramesByStreamId = getSubValues(pullFrames, Frame::getStreamId),
        getHeaderFramesByParentStreamId = getSubValues(headerFrames) { frame -> frame.header.parentStreamId },
        getMessageFramesByStreamId = getSubValues(messageFrames, Frame::getStreamId),
        getEndFramesByStreamId = getSubValues(endFrames, Frame::getStreamId),
    )
    val childHeaderFrames = ctx.getHeaderFramesByParentStreamId(0)
    val getStreamsByType = getStreams(ctx, myInfo.registerMap, childHeaderFrames)
    val onceInfo = getFramesByType(Frame.TypeCase.INFO).map { it.info }.take(1).singleOrError()
    return onceInfo
        .doOnSubscribe {
            //send myInfo
            val frame = Frame.newBuilder().setStreamId(0).setInfo(myInfo)
            ctx.sendFrame(frame.build())
        }
        .map { info ->
            val create = createChildChannel(ctx, 0, info.registerMap)
            Connection(remoteInfo = info, getStreamsByType = getStreamsByType, createChannel = create)
        }
}

internal fun createChildChannel(
    ctx: Context,
    parentStreamId: Int,
    remoteRegister: Map<String, Accept>,
    queueSize: Int = 1000,
): (header: Header.Builder) -> Channel = { header ->
    val dataSender = PublishSubject.create<ByteArray>()
    //缺少验证accept
    if (!remoteRegister.containsKey(header.streamType)) {
        val e = ProtocolError("the stream type is not accepted")
        Channel(
            onPull = Observable.error(e),
            onAvailable = Observable.error(e),
            dataSender = dataSender,
            getStreamsByType = { Observable.error(e) },
        )
    } else {
        val streamId = ctx.newStreamId()
        val messageSender = PublishSubject.create<Message>()
        val remoteError = ctx.getErrorFramesByStreamId(streamId)
            .take(1)
            .doOnNext { frame ->
                val e = frame.error
                throw RemoteError(e.type, e.message)
            }
            .share()
        val sentItemsWithNoError = messageSender
            .takeUntil(remoteError)
            .doOnNext { message ->
                val frame = Frame.newBuilder().setStreamId(streamId).setMessage(message)
                ctx.sendFrame(frame.build())
            }
            .doOnError { e ->
                val error = Error.newBuilder().setType(e.javaClass.name).setMessage(e.message)
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
        val theEnd = sentItemsWithNoError.ignoreElements().toObservable<Message>()
        val pulls = ctx.getPullFramesByStreamId(streamId)
            .map { it.pull }
            .takeUntil(remoteError)
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
                //send header
                val frame = Frame.newBuilder().setStreamId(streamId).setHeader(header.setParentStreamId(parentStreamId))
                ctx.sendFrame(frame.build())
            }
            .subscribe(
                { data ->
                    val message = Message.newBuilder().setData(ByteString.copyFrom(data)).build()
                    messageSender.onNext(message)
                },
                messageSender::onError,
                messageSender::onComplete
            )
        val childHeaderFrames = ctx.getHeaderFramesByParentStreamId(streamId)
        val getStreamsByType = getStreams(ctx, header.registerMap, childHeaderFrames)
        Channel(
            onPull = pulls,
            onAvailable = isAvailable,
            dataSender = dataSender,
            getStreamsByType = getStreamsByType,
        )
    }
}

internal fun getStreams(
    ctx: Context,
    register: Map<String, Accept>,
    childHeaderFrames: Observable<Frame>,
): (String) -> Observable<Stream> {
    val m = ConcurrentHashMap<String, Observable<Frame>>()
    val getHeaderFramesByStreamType = getSubValues(childHeaderFrames) { frame -> frame.header.streamType }
    register.forEach { (streamType, accept) ->
        //缺少验证accept
        //不确定被订阅的时机，先缓存
        val headerFrames = getHeaderFramesByStreamType(streamType)
            .cacheWithInitialCapacity(accept.maxConcurrentStream)
        m[streamType] = headerFrames
        //缺少takeUtil
        headerFrames.onErrorComplete().subscribe()
    }
    return { streamType ->
        val headerFrames = m[streamType] ?: Observable.error(ProtocolError("not acceptable stream type"))
        headerFrames.map { headerFrame -> createStream(ctx, headerFrame) }
    }
}

internal fun createStream(
    ctx: Context,
    headerFrame: Frame
): Stream {
    val streamId = headerFrame.streamId
    val header = headerFrame.header
    val theEnd = ctx.getEndFramesByStreamId(streamId)
        .take(1)
        .flatMap { frame ->
            if (frame.end.hasError()) Observable.error(Exception("cancel"))
            else Observable.just(frame)
        }
    val dataPuller = PublishSubject.create<Int>()
    val messages = ctx.getMessageFramesByStreamId(streamId)
        .map { frame -> frame.message }
        .takeUntil(theEnd)
        .takeUntil(dataPuller.ignoreElements().toObservable<Any>())
        .share()
    val pullIncrements = getPullIncrements(
        messages, dataPuller, ProtocolError("input buf overflow")
    )
    val pullFrames = pullIncrements
        .map { pull -> Frame.newBuilder().setPull(pull) }
        .concatWith(
            Observable.just(
                Frame.newBuilder().setError(Error.newBuilder().setType("Cancel"))
            )
        )
        .onErrorReturn { err ->
            val errType = err.javaClass.simpleName
            val message = err.message ?: ""
            Frame.newBuilder()
                .setError(Error.newBuilder().setType(errType).setMessage(message))
        }
        .map { builder -> builder.setStreamId(streamId).build() }
        .doOnNext { ctx.sendFrame(it) }
    pullFrames.subscribe() //side effect
    val onData = messages.map { it.data.toByteArray() }
    return Stream(
        header = header,
        onData = onData,
        dataPuller = dataPuller,
        createChannel = createChildChannel(ctx, streamId, header.registerMap),
        pipeChannels = pipeChannels(onData, dataPuller),
    )
}

internal fun pipeChannels(
    onData: Observable<ByteArray>,
    dataPuller: Observer<Int>,
): (PipeConfig) -> UnPipe {
    return { (channelMap, pullIncrement) ->
        val clearMap = channelMap.mapValues { (channel, outputData) ->
            val dataSender = channel.dataSender
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
    messages: Observable<Message>,
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
