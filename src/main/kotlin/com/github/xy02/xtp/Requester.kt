package com.github.xy02.xtp

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
}