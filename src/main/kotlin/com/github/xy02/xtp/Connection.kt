package com.github.xy02.xtp

import com.github.xy02.rx.getSubValues
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.core.Single
import xtp.Frame
import xtp.Request
import java.util.concurrent.atomic.AtomicInteger

//XTP连接，底层可使用任意有序的传输协议（例如TCP, WebSocket, QUIC）等
class Connection(
    val onFrame: Observable<Frame>,
    val frameSender: Observer<Frame>,
) {
    private val getFramesByType = onFrame.getSubValues { frame -> frame.typeCase }

    //    private val getFramesByType = getSubValues(socket.onFrame) { frame -> frame.typeCase }
    private val messageFrames = getFramesByType(Frame.TypeCase.MESSAGE)
    private val responseFrames = getFramesByType(Frame.TypeCase.RESPONSE)
    private val endFrames = getFramesByType(Frame.TypeCase.END)
    private val pullFrames = getFramesByType(Frame.TypeCase.PULL)
    private val cancelFrames = getFramesByType(Frame.TypeCase.CANCEL)
    private val fid = AtomicInteger(1)
    private val newFlowId = fid::getAndIncrement
    internal val watchMessageFrames = messageFrames.getSubValues(Frame::getFlowId)
    internal val watchResponseFrames = responseFrames.getSubValues(Frame::getFlowId)
    internal val watchEndFrames = endFrames.getSubValues(Frame::getFlowId)
    internal val watchPullFrames = pullFrames.getSubValues(Frame::getFlowId)
    internal val watchCancelFrames = cancelFrames.getSubValues(Frame::getFlowId)

    //    internal val watchMessageFrames = getSubValues(messageFrames, Frame::getFlowId)
    //    internal val watchResponseFrames = getSubValues(responseFrames, Frame::getFlowId)
    //    internal val watchEndFrames = getSubValues(endFrames, Frame::getFlowId)
    //    internal val watchPullFrames = getSubValues(pullFrames, Frame::getFlowId)
    //    internal val watchCancelFrames = getSubValues(cancelFrames, Frame::getFlowId)
    private val firstRemoteRequest = watchMessageFrames(0)
        .map { frame -> Request.parseFrom(frame.message) }
        .doOnNext { if (it.flowId <= 0) throw ProtocolError("first flowId must greater than 0") }
        .take(1).singleOrError()

    //收到对端根请求
    val onRootRequester: Single<Requester> = firstRemoteRequest.map { Requester(this, it) }.cache()

    //发送根请求（订阅后发送）
    fun sendRootRequest(req: Request.Builder): Single<Responder> =
        sendRequest(0, req)

    internal fun sendRequest(parentFlowId: Int, req: Request.Builder): Single<Responder> {
        val flowId = newFlowId()
        val request = req.setFlowId(flowId).build()
        val theResponse = watchResponseFrames(flowId)
        return theResponse.take(1).singleOrError()
            .doOnSubscribe {
                //发送request
                val messageOfRequest = request.toByteString()
                val firstFrame = Frame.newBuilder()
                    .setFlowId(parentFlowId)
                    .setMessage(messageOfRequest)
                frameSender.onNext(firstFrame.build())
            }
            .map { frame ->
                Responder(this, request, frame.response)
            }
            .cache()
    }

}