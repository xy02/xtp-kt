package com.github.xy02.example

import com.github.xy02.xtp.*
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.BehaviorSubject
import xtp.Accept
import xtp.Header
import xtp.PeerInfo
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) {
    //创建初始化函数
    val init = initWith(InfoHeader(
        peerInfo = PeerInfo.getDefaultInstance(),
    ))
    //创建TCP客户端Socket
    nioClientSocket(InetSocketAddress("localhost", 8001))
        .subscribeOn(Schedulers.newThread())
        .flatMap { socket ->
            println("onSocket")
            init(socket)
        }
        .retryWhen {
            it.flatMap { e ->
                println(e)
                Flowable.timer(3, TimeUnit.SECONDS)
            }
        }
        .subscribe(
            { conn ->
                println("onConnection")
                crazyAcc(conn)
            },
            { err -> err.printStackTrace() },
        )
    readLine()
}

private fun crazyAcc(conn: Connection) {
    conn.createChannel(
        Header.newBuilder()
            .setMessageType(typeAcc)
            .putRegister(typeAccReply, Accept.getDefaultInstance())
    ).subscribe { channel ->
        crazyAccReply(channel.getStreamByType(typeAccReply))
        Observable.timer(1, TimeUnit.SECONDS)
            .flatMap {
                channel.onPull.flatMap { pull ->
//                        println("the pull is $pull")
                    Observable.just(ByteArray(1))
                        .repeat(pull.toLong())
                }
            }
            .doOnComplete {
                //TODO 实现Ping流，并让断流后重启所有流
                println("doOnComplete")
            }
            .subscribe(channel.messageSender)
    }
}

private fun crazyAccReply(onStream: Single<Stream>) {
    onStream.subscribe { (header, bufs, bufPuller) ->
        var count = 0
        val d = Observable.interval(1, TimeUnit.SECONDS)
            .subscribe {
                println("${count / (it + 1)}/s")
            }
        bufs.scan(0) { acc, _ -> acc + 1 }
            .doOnNext { count = it }
            .subscribe({}, { d.dispose() })
//            val begin = System.currentTimeMillis()

        //验证请求
        println("onHeader:${header}\n")
        val pulls = bufs
//                .doOnNext { println("buf") }
            .map { 1 }
        Observable.merge(pulls, Observable.just(100000))
            .subscribe(bufPuller)
    }
}