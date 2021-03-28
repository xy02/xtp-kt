package com.github.xy02.xtp

import com.google.protobuf.ByteString
import xtp.Request
import xtp.Response

//流的响应者
class Responder internal constructor(
    //隶属连接
    val conn: Connection,
    //发送的请求
    val request: Request,
    //收到的响应
    val response: Response,
) {
    //收到的响应数据
    val data: ByteString = response.data

    //收到的消息流
    val flow: Flow? = if (response.hasHeader()) Flow(this) else null
}