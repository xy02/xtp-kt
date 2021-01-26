package com.github.xy02.xtp

import com.github.xy02.rx.getSubValues
import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.Maybe
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.AsyncSubject
import io.reactivex.rxjava3.subjects.PublishSubject
import net.i2p.crypto.eddsa.EdDSAEngine
import net.i2p.crypto.eddsa.EdDSAPrivateKey
import net.i2p.crypto.eddsa.EdDSAPublicKey
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveTable
import net.i2p.crypto.eddsa.spec.EdDSAPrivateKeySpec
import net.i2p.crypto.eddsa.spec.EdDSAPublicKeySpec
import xtp.*
import java.security.MessageDigest
import java.util.*
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
    val remoteAppInfo: AppInfo? = null,
)

data class Channel(
    val onPull: Observable<Int>,
    val onAvailable: Observable<Boolean>,
    val dataSender: Observer<ByteArray>,
    val getStreamsByType: (String) -> Observable<Stream>,
    val onResult: Maybe<ByteArray>,
)

data class Stream(
    val header: Header,
    val onData: Observable<ByteArray>,
    val dataPuller: Observer<Int>,
    val closeWithData: (ByteArray) -> Unit,
    val createChannel: (header: Header.Builder) -> Channel,
    val pipeChannels: (PipeConfig) -> UnPipe,
)

data class PipeConfig(
    val channelMap: Map<Channel, Observable<ByteArray>>,
    val pullIncrement: Int = 10000,
)

data class InitOptions(
    val denyAnonymousApp: Boolean = false,
    val appPublicKeyMap: Map<String, ByteArray> = mapOf(),
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

data class VerificationFailedError(
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
    val getCloseFramesByStreamId: (Int) -> Observable<Frame>,
    val getPullFramesByStreamId: (Int) -> Observable<Frame>,
    val getHeaderFramesByParentStreamId: (Int) -> Observable<Frame>,
    val getEndFramesByStreamId: (Int) -> Observable<Frame>,
    val getMessageFramesByStreamId: (Int) -> Observable<Frame>,
)

fun initWith(onMyInfo: Single<Info>, options: InitOptions = InitOptions()): (Socket) -> Single<Connection> {
    val singleMyInfoPair = onMyInfo.map { myInfo ->
        val bytes = Frame.newBuilder().setStreamId(0).setInfo(myInfo).build().toByteArray()
        Pair(bytes, myInfo)
    }.cache()
    return { socket ->
        singleMyInfoPair.flatMap { (myInfoBytes, myInfo) ->
            val sendEndWithError = { err: Throwable, streamId: Int ->
                val end = End.newBuilder().setError(
                    Error.newBuilder()
                        .setType(err.javaClass.name)
                        .setMessage(err.message)
                )
                val frame = Frame.newBuilder().setStreamId(streamId).setEnd(end).build()
                socket.bytesSender.onNext(frame.toByteArray())
            }
            val frames = socket.onBytes.map(Frame::parseFrom).share()
            val getFramesByType = getSubValues(frames) { frame -> frame.typeCase }
            val headerFrames = getFramesByType(Frame.TypeCase.HEADER)
            val messageFrames = getFramesByType(Frame.TypeCase.MESSAGE)
            val endFrames = getFramesByType(Frame.TypeCase.END)
            val pullFrames = getFramesByType(Frame.TypeCase.PULL)
            val closeFrames = getFramesByType(Frame.TypeCase.CLOSE)
            val sid = AtomicInteger()
            val ctx = Context(
                newStreamId = { sid.getAndIncrement() },
                sendFrame = { frame: Frame -> socket.bytesSender.onNext(frame.toByteArray()) },
                getCloseFramesByStreamId = getSubValues(closeFrames, Frame::getStreamId),
                getPullFramesByStreamId = getSubValues(pullFrames, Frame::getStreamId),
                getHeaderFramesByParentStreamId = getSubValues(headerFrames) { frame -> frame.header.parentStreamId },
                getMessageFramesByStreamId = getSubValues(messageFrames, Frame::getStreamId),
                getEndFramesByStreamId = getSubValues(endFrames, Frame::getStreamId),
            )
            val childHeaderFrames = ctx.getHeaderFramesByParentStreamId(0)
            val getStreamsByType = getStreams(ctx, myInfo.registerMap, childHeaderFrames)
            val singleRemoteInfo = getFramesByType(Frame.TypeCase.INFO).map { it.info }.take(1).singleOrError()
            singleRemoteInfo
                .doOnSubscribe {
                    //send myInfo
                    socket.bytesSender.onNext(myInfoBytes)
                }
                .flatMap { remoteInfo ->
                    val create = createChildChannel(ctx, 0, remoteInfo.registerMap)
                    if (remoteInfo.appCert.serializedSize == 0 && !options.denyAnonymousApp)
                        Single.just(
                            Connection(
                                remoteInfo = remoteInfo,
                                getStreamsByType = getStreamsByType, createChannel = create
                            )
                        )
                    else getValidAppInfo(remoteInfo.appCert, options.appPublicKeyMap)
                        .map { appInfo ->
                            Connection(
                                remoteInfo = remoteInfo, getStreamsByType = getStreamsByType,
                                createChannel = create, remoteAppInfo = appInfo
                            )
                        }
                        .doOnError { err ->
                            sendEndWithError(err, 0)
                            //关闭连接
                            socket.bytesSender.onComplete()
                        }
                }
        }
    }
}

fun getValidAppInfo(appCert: AppCert, appPublicKeyMap: Map<String, ByteArray>): Single<AppInfo> {
    return Single.create { emitter ->
        val appInfo = AppInfo.parseFrom(appCert.appInfo)
        val pk = appPublicKeyMap[appInfo.name] ?: throw VerificationFailedError()
        val spec = EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.ED_25519)
        val keySpec = EdDSAPublicKeySpec(pk, spec)
        val key = EdDSAPublicKey(keySpec)
        //Signature sgr = Signature.getInstance("EdDSA", "I2P");
        val sgr = EdDSAEngine(MessageDigest.getInstance(spec.hashAlgorithm))
        sgr.initVerify(key)
        val infoBytes = appCert.appInfo.toByteArray()
        sgr.update(infoBytes)
        val ok = sgr.verify(appCert.appSign.toByteArray())
        if (ok) emitter.onSuccess(appInfo) else throw VerificationFailedError()
    }
}

fun makeAppCertWithEd25519(appInfo: AppInfo, seed: ByteArray): Single<AppCert> {
    return Single.create { emitter ->
        if (appInfo.name.isNullOrEmpty()) {
            emitter.onError(Exception("require name of appInfo"))
            return@create
        }
        val spec = EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.ED_25519)
        val keySpec = EdDSAPrivateKeySpec(seed, spec)
        val key = EdDSAPrivateKey(keySpec)
        val sgr = EdDSAEngine(MessageDigest.getInstance(spec.hashAlgorithm))
        sgr.initSign(key)
        val info = appInfo.toByteString()
        sgr.update(info.toByteArray())
        val sig = sgr.sign()
        val cert = AppCert.newBuilder()
            .setAppInfo(info)
            .setAppSign(ByteString.copyFrom(sig))
            .setSignAlg(SignAlg.Ed25519)
            .build()
        emitter.onSuccess(cert)
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
            onResult = Maybe.error(e),
        )
    } else {
        val streamId = ctx.newStreamId()
        val messageSender = PublishSubject.create<Message>()
        val remoteClose = ctx.getCloseFramesByStreamId(streamId)
            .take(1)
            .doOnNext { frame ->
                if (frame.close.hasError()) {
                    val e = frame.close.error
                    throw RemoteError(e.type, e.message)
                }
            }
        val onResult = remoteClose.filter { frame -> frame.close.hasMessage() }
            .map { frame -> frame.close.message.data.toByteArray() }
            .singleElement().cache()
        val sentItemsWithNoError = messageSender
            .takeUntil(onResult.toObservable())
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
            onResult = onResult,
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
    val resultSubject = AsyncSubject.create<Close>()
    val dataPuller = PublishSubject.create<Int>()
    val sendDataResult = { data: ByteArray ->
        val message = Message.newBuilder().setData(ByteString.copyFrom(data))
        val close = Close.newBuilder().setMessage(message)
        resultSubject.onNext(close.build())
        dataPuller.onComplete()
    }
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
        .doOnComplete { resultSubject.onComplete() }
        //缺少功能：等待所有子流生命周期结束后才能发送close
        .concatWith(
            resultSubject.single(Close.getDefaultInstance())
                .map { close -> Frame.newBuilder().setClose(close) }
        )
        .onErrorReturn { err ->
            val errType = err.javaClass.simpleName
            val message = err.message ?: ""
            val close = Close.newBuilder().setError(Error.newBuilder().setType(errType).setMessage(message))
            Frame.newBuilder().setClose(close)
        }
        .map { builder -> builder.setStreamId(streamId).build() }
        .doOnNext { ctx.sendFrame(it) }
    pullFrames.subscribe() //side effect
    val onData = messages.map { it.data.toByteArray() }
    return Stream(
        header = header,
        onData = onData,
        dataPuller = dataPuller,
        closeWithData = sendDataResult,
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
