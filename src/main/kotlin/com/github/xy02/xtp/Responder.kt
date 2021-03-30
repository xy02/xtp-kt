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
    val type: String = request.type
    private val singleResponse = SingleSubject.create<Response>()
    private val maybeChannel = singleResponse.flatMapMaybe { res ->
        if (res.hasHeader())
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
    fun reply(response: Response) = singleResponse.onSuccess(response)

    //响应错误
    fun replyError(e: Throwable) {
        val response = Response.newBuilder().setEnd(
            End.newBuilder().setError(
                Error.newBuilder().setType(e.javaClass.name)
                    .setStrMessage(e.message ?: "")
            )
        ).build()
        reply(response)
    }

    //创建响应通道用于发送流消息，订阅后发送response
    fun createResponseChannel(response: Response.Builder): Single<Channel> {
        if (singleResponse.hasValue())
            return Single.error(ProtocolError("one request, one response"))
        if (!response.hasHeader())
            response.header = Header.getDefaultInstance()
        singleResponse.onSuccess(response.build())
        return maybeChannel.toSingle()
    }
}