package com.github.xy02.example

import com.github.xy02.xtp.*
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
    val s = BehaviorSubject.create<Int>()
    s.onNext(1)
    s.onComplete()
    s.subscribe { println("s:$it") }
    val infoHeader = InfoHeader(
        peerInfo = PeerInfo.getDefaultInstance(),
    )
    val init = initWith(infoHeader)
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
    val reply = "accReply"
    val channel = conn.createChannel(
        Header.newBuilder()
            .setHandlerName("acc")
            .putRegister(reply, Accept.getDefaultInstance())
    )
    crazyAccReply(channel.getStreamByType(reply))
    Observable.timer(1, TimeUnit.SECONDS)
        .flatMap {
            channel.onPull.flatMap { pull ->
//                        println("the pull is $pull")
                Observable.just(ByteArray(1))
                    .repeat(pull.toLong())
            }
        }
        .subscribe(channel.messageSender)
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