package com.github.xy02.example

import com.github.xy02.xtp.*
import java.text.SimpleDateFormat

fun test() {
    val theConn = init()
    theConn.subscribe { conn ->
        handleAcc(conn)
    }
}

fun handleAcc(conn: Connection) {
    val onAccRequest = conn.getStreamsByType("Acc")
    onAccRequest.subscribe { stream ->
        //验证请求
        println("onHeader:${stream.header}")
        val df = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        //处理上游发来的数据（未向上游拉取数据时是不会收到数据的）
        val handledData = stream.onData
            .scan(0) { acc, _ -> acc + 1 }
            .map { acc ->
                val json: JsonObject = JsonObject()
                    .put("time", df.format(System.currentTimeMillis()))
                    .put("acc", acc)
                json.toBuffer().bytes
            }
        //简化以下代码
        stream.pipeChannels(
            PipeConfig(
                mapOf(stream.createChannel() to handledData)
            )
        )
//        //创建下游流（会发送header）
//        val accReplyChannel=stream.createChannel()
//        //向下游输出处理过的数据
//        handledData.subscribe(accReplyChannel.dataSender)
//        //让拉取上游数据的速度与下游流的拉取速度相同
//        accReplyChannel.onPull.subscribe(stream.dataPuller)
    }
}