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
import java.util.concurrent.atomic.AtomicInteger

data class Socket(
    val onBytes: Observable<ByteArray>,
    val bytesSender: Observer<ByteArray>,
)

data class Connection(
    val remoteInfo: Info,
    val getStreamsByType: (String) -> Observable<Stream>,
    //创建流
    val createChannel: (header: Header.Builder) -> Single<Channel>,
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
    val createChannel: (header: Header.Builder) -> Single<Channel>,
)

data class RemoteError(
    val type: String,
    override val message: String,
) : Throwable()

data class ProtocolError(
    override val message: String = "",
) : Throwable()

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
    val getStreamsByType = getStreams(myInfo.registerMap, childHeaderFrames, ctx)
    val onceInfo = getFramesByType(Frame.TypeCase.INFO).map { it.info }.take(1).singleOrError()
    return onceInfo.map { info ->
        val create = createChildChannel(ctx, 0, info.registerMap)
        Connection(remoteInfo = info, getStreamsByType = getStreamsByType, createChannel = create)
    }
}

internal fun createChildChannel(
    ctx: Context,
    parentStreamId: Int,
    remoteRegister: Map<String, Accept>,
): (header: Header.Builder) -> Single<Channel> = { header ->
    //缺少验证accept
    if (!remoteRegister.containsKey(header.streamType))
        Single.error(ProtocolError("the stream type is not accepted"))
    else {
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
        val dataSender = PublishSubject.create<ByteArray>()
        val childHeaderFrames = ctx.getHeaderFramesByParentStreamId(streamId)
        val getStreamsByType = getStreams(header.registerMap, childHeaderFrames, ctx)
        Single.just(
            Channel(
                onPull = pulls,
                onAvailable = isAvailable,
                dataSender = dataSender,
                getStreamsByType = getStreamsByType,
            )
        ).doOnSubscribe {
            //side effect
            dataSender.withLatestFrom(isAvailable,
                { data, ok ->
                    if (ok) {
                        val message = Message.newBuilder().setData(ByteString.copyFrom(data)).build()
                        messageSender.onNext(message)
                    }
                })
                .subscribe(
                    {},
                    messageSender::onError,
                    messageSender::onComplete
                )
            //send header
            val frame = Frame.newBuilder().setStreamId(streamId).setHeader(header.setParentStreamId(parentStreamId))
            ctx.sendFrame(frame.build())
        }
    }
}

internal fun getStreams(
    register: Map<String, Accept>,
    childHeaderFrames: Observable<Frame>,
    ctx: Context,
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
    return Stream(
        header = header,
        onData = messages.map { it.data.toByteArray() },
        dataPuller = dataPuller,
        createChannel = createChildChannel(ctx, streamId, header.registerMap),
    )
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
