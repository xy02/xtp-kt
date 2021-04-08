package com.github.xy02.example

import com.github.xy02.xtp.Channel
import com.github.xy02.xtp.Flow
import com.github.xy02.xtp.PipeSetup
import com.github.xy02.xtp.nioServer
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.plugins.RxJavaPlugins
import xtp.Header
import java.text.SimpleDateFormat

fun main(args: Array<String>) {
    RxJavaPlugins.setErrorHandler { e -> println("RxJavaPlugins e:$e") }
    //创建TCP服务端
    nioServer()
        .flatMapSingle { conn ->
            println("onConnection")
            conn.singleRootFlow
        }
        .flatMapCompletable(::handleClientInfo)
        .subscribe(
            { },
            { err -> err.printStackTrace() },
        )
    readLine()
}

fun handleClientInfo(rootFlow: Flow): Completable {
    println("handleClientInfo")
    //验证收到的header.info，略
    val conn = rootFlow.conn
    //发送根流头
    val header = Header.newBuilder().setInfoType("ServiceInfo")
    return conn.sendRootHeader(header)
        .flatMapCompletable { rootChannel ->
            service(rootFlow, rootChannel)
        }
        .onErrorComplete()
}

fun service(rootFlow: Flow, rootChannel: Channel): Completable {
    println("onService")
    return rootFlow.onChildFlow
        .doOnSubscribe {
            //拉取“流”
            rootChannel.onPull.subscribe(rootFlow.messagePuller)
        }
        .flatMapCompletable { flow ->
            when (flow.header.infoType) {
                "Acc" -> acc(flow, rootChannel)
                else -> Completable.complete()
            }
        }
}

//累加收到的请求个数，响应json字符串，形如{"time":"2021-03-01 10:31:59","acc":13}
private fun acc(flow: Flow, rootChannel: Channel): Completable {
    //处理新流，验证header.info等
    println("onHeader:${flow.header}")
    //创建下游流
    return rootChannel
        .sendHeader(
            Header.newBuilder().setInfoType("AccReply")
        )
        .flatMapCompletable { accReplyChannel ->
            //处理上游发来的数据（未向上游拉取数据时是不会收到数据的）
            val df = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val handledMessage = flow.onMessage
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
            //向下游输出处理过的消息
            handledMessage.subscribe(accReplyChannel.messageSender)
            //自动流量控制
            flow.pipeChannels(
                //可以有多个下游管道
                mapOf(accReplyChannel to PipeSetup())
            )
//            //让拉取上游数据的速度与下游流的拉取速度相同
//            accReplyChannel.onPull.subscribe(flow.messagePuller)
        }
}
