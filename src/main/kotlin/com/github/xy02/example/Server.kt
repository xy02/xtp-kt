package com.github.xy02.example

import com.github.xy02.xtp.Connection
import com.github.xy02.xtp.PipeSetup
import com.github.xy02.xtp.init
import com.github.xy02.xtp.nioServerSockets
import io.reactivex.rxjava3.plugins.RxJavaPlugins
import io.reactivex.rxjava3.schedulers.Schedulers
import xtp.Header
import java.text.SimpleDateFormat

fun main(args: Array<String>) {
    RxJavaPlugins.setErrorHandler { e -> println("RxJavaPlugins e:$e") }
    //创建TCP服务端Sockets
    nioServerSockets()
        .subscribeOn(Schedulers.newThread())//如果是安卓，需另起线程
        .flatMapSingle { socket ->
            println("onSocket")
            //转换Socket->Connection
            init(Header.newBuilder(), socket)
        }
        .subscribe(
            { conn ->
                println("onConnection")
                //业务函数
                acc(conn)
            },
            { err -> err.printStackTrace() },
        )
    readLine()
}

//累加收到的数据个数，并向下游流输出json字符串
// {"time":"2021-03-01 10:31:59","acc":13}
private fun acc(conn: Connection) {
    //订阅消息流
    val onFlow = conn.flow.getChildFlowByFn("acc")
    conn.flow.messagePuller.onNext(10)
    //处理新流
    onFlow.onErrorComplete()
        .flatMapSingle { flow ->
            //验证请求，处理header.info等
            println("onHeader:${flow.header}\n")
            //创建下游流
            conn.channel
                .createChildChannel(
                    Header.newBuilder().setFn("accReply")
                )
                .map { channel -> Pair(flow, channel) }
        }
        .flatMapCompletable { (accFlow, accReplyChannel) ->
            //处理上游发来的数据（未向上游拉取数据时是不会收到数据的）
            val df = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val handledData = accFlow.onMessage
                .scan(0) { acc, _ -> acc + 1 }
                .map { acc ->
                    val json = """{"time":${df.format(System.currentTimeMillis())},"acc":$acc}"""
                    json.toByteArray()
//                val json = io.vertx.core.json.JsonObject()
//                    .put("time", df.format(System.currentTimeMillis()))
//                    .put("acc", acc)
//                json.toBuffer().bytes
//                val bb = java.nio.ByteBuffer.allocate(4)
//                bb.putInt(acc)
//                bb.array()
                }
            //向下游输出处理过的数据
            handledData.subscribe(accReplyChannel.dataSender)
            //自动流量控制
            accFlow.pipeChannels(
                //可以有多个下游管道
                mapOf(accReplyChannel to PipeSetup())
            )
//        //让拉取上游数据的速度与下游流的拉取速度相同
//        accReplyChannel.onPull.subscribe(accFlow.messagePuller)
        }
        .onErrorComplete()
        .subscribe()
}
