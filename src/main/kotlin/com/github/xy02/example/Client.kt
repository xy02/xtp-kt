package com.github.xy02.example

import com.github.xy02.xtp.Connection
import com.github.xy02.xtp.Stream
import com.github.xy02.xtp.initWith
import com.github.xy02.xtp.nioClientSocket
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.schedulers.Schedulers
import xtp.Accept
import xtp.Header
import xtp.Info
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) {
    val singleMyInfo = Single.just(Info.getDefaultInstance())
    val init = initWith(singleMyInfo)
    val theSocket = nioClientSocket(InetSocketAddress("localhost", 8001)).toObservable()
    theSocket
        .subscribeOn(Schedulers.newThread())
        .flatMapSingle { socket ->
            println("onSocket")
            init(socket)
        }
        .retryWhen {
            it.flatMap { e ->
                println(e)
                Observable.timer(3, TimeUnit.SECONDS)
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