package com.github.xy02.xtp

import com.github.xy02.rx.getSubValues
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.subjects.SingleSubject
import xtp.Frame
import xtp.Header
import java.util.concurrent.atomic.AtomicInteger

//XTP连接，底层可使用任意有序的传输协议（例如TCP, WebSocket, QUIC）等
class Connection(
    val onFrame: Observable<Frame>,
    val frameSender: Observer<Frame>,
) {
    private val getFramesByType = onFrame.getSubValues { frame -> frame.typeCase }

    //    private val getFramesByType = getSubValues(socket.onFrame) { frame -> frame.typeCase }
    private val messageFrames = getFramesByType(Frame.TypeCase.MESSAGE)
    private val endFrames = getFramesByType(Frame.TypeCase.END)
    private val pullFrames = getFramesByType(Frame.TypeCase.PULL)
    private val cancelFrames = getFramesByType(Frame.TypeCase.CANCEL)
    private val fid = AtomicInteger(1)
    internal val newFlowId = fid::getAndIncrement
    internal val watchMessageFrames = messageFrames.getSubValues(Frame::getFlowId)
    internal val watchEndFrames = endFrames.getSubValues(Frame::getFlowId)
    internal val watchPullFrames = pullFrames.getSubValues(Frame::getFlowId)
    internal val watchCancelFrames = cancelFrames.getSubValues(Frame::getFlowId)

    private val firstRemoteHeader = watchMessageFrames(0)
        .map { frame -> Header.parseFrom(frame.message) }
        .doOnNext { if (it.flowId <= 0) throw ProtocolError("first flowId must greater than 0") }
        .take(1).singleOrError()

    //收到对端的根流（第一个流）
    val singleRootFlow: Single<Flow> = firstRemoteHeader.map { Flow(this, it) }.cache()

    //根流头发送器
    private val rootHeaderSender = SingleSubject.create<Header.Builder>()

    //发送根流头后得到的通道
    val singleRootChannel: Single<Channel> = rootHeaderSender
        .doOnSuccess {header->
            if (header.infoType.isNullOrEmpty())
                throw ProtocolError("require infoType")
        }
        .map { it.setFlowId(newFlowId()).build() }
        .map { Channel(this, it) }
        .doOnSuccess {
            val message = it.header.toByteString()
            val frame = Frame.newBuilder().setFlowId(0).setMessage(message).build()
            frameSender.onNext(frame)
        }
        .cache()

    init {
        singleRootChannel.subscribe()
    }

    //发送根流头
    fun sendRootHeader(header:Header.Builder):Single<Channel>{
        rootHeaderSender.onSuccess(header)
        return singleRootChannel
    }
}