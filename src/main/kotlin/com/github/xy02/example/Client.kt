package com.github.xy02.example

import com.github.xy02.xtp.*
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.schedulers.Schedulers
import xtp.Accept
import xtp.Header
import xtp.Info
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) {
    val myInfo = Info.newBuilder()
        .putRegister("xx", Accept.newBuilder().setMaxConcurrentStream(10).build())
        .build()
    val source = nioSocketsSource()
    val theSocket = nioClientSocket(source, InetSocketAddress("localhost", 8001)).toObservable()
    theSocket
        .flatMapSingle { socket ->
            println("onSocket")
            init(socket, myInfo)
        }
        .retryWhen {
            it.flatMap { e ->
                println(e)
                Observable.timer(3, TimeUnit.SECONDS)
            }
        }
        .subscribeOn(Schedulers.newThread())
        .doOnComplete { println("conn onComplete") }
        .subscribe(
            { conn ->
                println("onConnection")
                crazyAcc(conn)
            },
            { err -> err.printStackTrace() },
        )
    Thread.sleep(1000000000)
}

private fun crazyAcc(conn: Connection) {
    val reply = "AccReply"
    val channel = conn.createChannel(
        Header.newBuilder()
            .setStreamType("Acc")
            .putRegister(reply, Accept.newBuilder().setMaxConcurrentStream(10).build())
    )
    crazyAccReply(channel.getStreamsByType(reply))
    Observable.timer(1, TimeUnit.SECONDS)
        .flatMap {
            channel.onPull.flatMap { pull ->
//                        println("the pull is $pull")
                Observable.just(ByteArray(1))
                    .repeat(pull.toLong())
            }
        }
        .subscribe(channel.dataSender)
}

private fun crazyAccReply(onStream: Observable<Stream>) {
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
        println("onHeader:${header.streamType}")
        val pulls = bufs
//                .doOnNext { println("buf") }
            .map { 1 }
        Observable.merge(pulls, Observable.just(100000))
            .subscribe(bufPuller)
    }
}