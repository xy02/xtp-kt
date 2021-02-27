package com.github.xy02.example

import com.github.xy02.xtp.*
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.plugins.RxJavaPlugins
import io.reactivex.rxjava3.schedulers.Schedulers
import xtp.Accept
import xtp.Header
import xtp.PeerInfo
import java.text.SimpleDateFormat

fun main(args: Array<String>) {
    RxJavaPlugins.setErrorHandler { e -> println("RxJavaPlugins e:$e") }
    val sockets = nioServerSockets()
    val infoHeader = InfoHeader(
        peerInfo = PeerInfo.getDefaultInstance(),
        register = mapOf("Acc" to Accept.getDefaultInstance())
    )
    val init = initWith(Single.just(infoHeader))
    sockets.flatMapMaybe { socket ->
        println("onSocket")
        init(socket)
            .doOnError { err -> println("init err:$err") }
            .onErrorComplete()
    }.subscribeOn(Schedulers.newThread())
        .subscribe(
            { conn ->
                println("onConnection")
                acc(conn)
            },
            { err -> err.printStackTrace() },
        )
    readLine()
}

//累加收到的数据个数
private fun acc(conn: Connection) {
    val onStream = conn.getStreamByType("Acc")
    onStream.onErrorComplete().subscribe { stream ->
        //验证请求
        println("onHeader:${stream.header}\n")
        val df = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        //处理上游发来的数据（未向上游拉取数据时是不会收到数据的）
        val handledData = stream.onMessage
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
        //创建下游流（会发送header）
        val accReplyChannel = stream.createChannel(Header.newBuilder().setStreamName("AccReply"))
        //流量控制
        stream.pipeChannels(
            PipeConfig(
                mapOf(accReplyChannel to handledData)
            )
        )
//        //向下游输出处理过的数据
//        handledData.subscribe(accReplyChannel.dataSender)
//        //让拉取上游数据的速度与下游流的拉取速度相同
//        accReplyChannel.onPull.subscribe(stream.dataPuller)
    }
}
