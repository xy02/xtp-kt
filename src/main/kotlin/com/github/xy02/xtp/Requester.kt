package com.github.xy02.xtp

import io.reactivex.rxjava3.core.Maybe
import xtp.Request
import xtp.Response

//流的请求者
class Requester internal constructor(
    //隶属连接
    val conn: Connection,
    //发送的请求
    val request: Request,
    //收到的响应
    val response: Response,
) {
    //收到的消息流
    val flow: Flow? = if (response.success.hasHeader()) Flow(this) else null

    //新的响应器，收到的应答可能是新请求
    val maybeNewResponder = Maybe.create<Responder> { emitter ->
        if (response.hasError())
            return@create emitter.onComplete()
        try {
            val req = Request.parseFrom(response.success.data)
            if (req.flowId <= 0)
                return@create emitter.onComplete()
            val responder = Responder(conn, req)
            emitter.onSuccess(responder)
        } catch (e: Exception) {
            emitter.onComplete()
        }
    }
}