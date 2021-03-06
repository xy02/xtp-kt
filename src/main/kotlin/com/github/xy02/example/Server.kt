package com.github.xy02.example

import com.github.xy02.xtp.*
import io.reactivex.rxjava3.plugins.RxJavaPlugins
import io.reactivex.rxjava3.schedulers.Schedulers
import xtp.Accept
import xtp.Header
import xtp.PeerInfo
import java.text.SimpleDateFormat

val typeAcc = "Acc"
val typeAccReply = "AccReply"

fun main(args: Array<String>) {
    RxJavaPlugins.setErrorHandler { e -> println("RxJavaPlugins e:$e") }
    //创建初始化函数
    val init = initWith(
        InfoHeader(
            //可包含自身身份证明等信息
            peerInfo = PeerInfo.getDefaultInstance(),
            //注册可接收的messageType
            register = mapOf(typeAcc to Accept.getDefaultInstance())
        )
    )
    //创建TCP服务端Sockets
    nioServerSockets()
        .subscribeOn(Schedulers.newThread())
        .flatMapMaybe { socket ->
            println("onSocket")
            //转换Socket->Connection
            init(socket)
                .doOnError { err -> println("init err:$err") }
                .onErrorComplete()
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
    //获取消息流
    val onStream = conn.getStreamByType(typeAcc)
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
        //创建下游流
        val accReplyChannel = stream.createChannel(
            Header.newBuilder().setMessageType(typeAccReply)
        )
        //向下游输出，自动流量控制
        stream.pipeChannels(
            //可以有多个下游管道
            mapOf(accReplyChannel to handledData.map { HandledMessage(it) })
        )
//        accReplyChannel.subscribe {channel->
//            //向下游输出处理过的数据
//            handledData.subscribe(channel.messageSender)
//            //让拉取上游数据的速度与下游流的拉取速度相同
//            channel.onPull.subscribe(stream.messagePuller)
//        }
    }
}
