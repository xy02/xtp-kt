package com.github.xy02.example

import com.github.xy02.xtp.Connection
import com.github.xy02.xtp.Flow
import com.github.xy02.xtp.init
import com.github.xy02.xtp.nioClientSocket
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.schedulers.Schedulers
import xtp.Header
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) {
    val apiNumber = 1
    //创建TCP客户端Socket
    nioClientSocket(InetSocketAddress("localhost", 8001))
        .subscribeOn(Schedulers.newThread())
        .flatMap { socket ->
            println("onSocket")
            init(Header.newBuilder(), socket)
        }
        .retryWhen {
            it.flatMap { e ->
                println(e)
                Flowable.timer(3, TimeUnit.SECONDS)
            }
        }
        .flatMap { conn ->
            println("onConnection")
            conn.channel.onPull
                .take(1)
                .singleOrError()
                .map { pull ->
                    println("pull:$pull")
                    if (pull < apiNumber)
                        throw Exception("not enough pull")
                    crazyAcc(conn)
                }
        }
        .subscribe(
            {},
            { err -> err.printStackTrace() },
        )
    readLine()
}

private fun crazyAcc(conn: Connection) {
    //订阅应答流
    crazyAccReply(conn.flow.getSingleChildFlowByFn("accReply"))
    conn.flow.messagePuller.onNext(1)
    //新建请求流
    conn.channel.createChildChannel(
        Header.newBuilder().setFn("acc")
    ).subscribe { channel ->
        Observable.timer(1, TimeUnit.SECONDS)
            .flatMap {
                channel.onPull.flatMap { pull ->
//                        println("the pull is $pull")
                    Observable.just(ByteArray(1))
                        .repeat(pull.toLong())
                }
            }
            .doOnComplete {
                println("doOnComplete")
            }
            .subscribe(channel.dataSender)
    }
}

private fun crazyAccReply(singleFlow: Single<Flow>) {
    singleFlow.subscribe { flow ->
        var count = 0
        val d = Observable.interval(1, TimeUnit.SECONDS)
            .subscribe {
                println("${count / (it + 1)}/s")
            }
        flow.onData
            .scan(0) { acc, _ -> acc + 1 }
            .doOnNext { count = it }
            .subscribe({}, { d.dispose() })
//            val begin = System.currentTimeMillis()

        //验证请求
        println("onHeader:${flow.header}\n")
        val pulls = flow.onData
//                .doOnNext { println("buf") }
            .map { 1 }
        Observable.merge(pulls, Observable.just(100000))
            .subscribe(flow.messagePuller)
    }
}