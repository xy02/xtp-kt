package com.github.xy02.xtp

import io.reactivex.rxjava3.core.Maybe
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.subjects.SingleSubject
import xtp.*

//流的响应者
class Responder internal constructor(
    //隶属连接
    val conn: Connection,
    //收到的请求
    val request: Request,
) {
    private val singleResponse = SingleSubject.create<Response>()
    private val maybeChannel = singleResponse.flatMapMaybe { res ->
        if (res.hasSuccess() && res.success.hasHeader())
            Maybe.just(Channel(this, res))
        else Maybe.empty()
    }

    init {
        maybeChannel
            .doOnComplete {
                //发送不是header的response
                val frame = Frame.newBuilder()
                    .setFlowId(request.flowId)
                    .setResponse(singleResponse.value)
                conn.frameSender.onNext(frame.build())
            }
            .subscribe()
    }

    //响应
    fun reply(response: Response) {
        if (singleResponse.hasValue())
            throw ProtocolError("one request, one response")
        singleResponse.onSuccess(response)
    }

    //响应错误
    fun replyError(e: Throwable) {
        val response = Response.newBuilder().setError(
            Error.newBuilder()
                .setClassName(e.javaClass.name)
                .setStrMessage(e.message ?: "")
        ).build()
        reply(response)
    }

    //响应成功，无流头信息
    fun replySuccess(success: Success.Builder) {
        if (success.hasHeader())
            success.clearHeader()
        val response = Response.newBuilder().setSuccess(success).build()
        reply(response)
    }

    //响应请求
    fun replyRequest(req: Request.Builder): Single<Requester> {
        val flowId = conn.newFlowId()
        val request = req.setFlowId(flowId).build()
        val theResponse = conn.watchResponseFrames(flowId)
        return theResponse.take(1).singleOrError()
            .doOnSubscribe {
                //发送request
                val success = Success.newBuilder()
                    .setDataClass(Request::javaClass.name)
                    .setData(request.toByteString())
                replySuccess(success)
            }
            .map { frame ->
                Requester(conn, request, frame.response)
            }
            .cache()
    }

    //创建响应通道用于发送流消息，订阅后发送response
    fun createResponseChannel(success: Success.Builder): Single<Channel> {
        if (singleResponse.hasValue())
            return Single.error(ProtocolError("one request, one response"))
        if (!success.hasHeader())
            success.header = Header.getDefaultInstance()
        val response = Response.newBuilder().setSuccess(success).build()
        singleResponse.onSuccess(response)
        return maybeChannel.toSingle()
    }

}