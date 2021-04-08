package com.github.xy02.example

import com.github.xy02.xtp.Channel
import com.github.xy02.xtp.Flow
import com.github.xy02.xtp.nioClient
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.schedulers.Schedulers
import xtp.Header
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) {
    //创建TCP客户端
    nioClient(InetSocketAddress("localhost", 8001))
        .subscribeOn(Schedulers.newThread())
        .retryWhen {
            it.flatMap { e ->
                println(e)
                Flowable.timer(3, TimeUnit.SECONDS)
            }
        }
        .flatMap { conn ->
            println("onConnection")
            //发送根流头
            val header = Header.newBuilder().setInfoType("ClientInfo")
            conn.sendRootHeader(header)
        }
        .flatMapCompletable { rootChannel ->
            rootChannel.conn.singleRootFlow
                .flatMapCompletable { rootFlow ->
                    onServiceReply(rootChannel, rootFlow)
                }
        }
        .subscribe(
            { println("complete") },
            { err -> err.printStackTrace() },
        )
    readLine()
}

fun onServiceReply(rootChannel: Channel, rootFlow: Flow): Completable {
    println("onServiceReply")
    return Completable.mergeArray(
        //输入
        rootFlow.onChildFlow
            .doOnSubscribe {
                //接收10条子流
                rootFlow.pull(10)
            }
            .flatMapCompletable { flow ->
                when (flow.header.infoType) {
                    "AccReply" -> handleAccReply(flow)
                    else -> Completable.complete()
                }
            },
        //输出
        crazyAcc(rootChannel),
    ).onErrorComplete()
}

fun handleAccReply(flow: Flow): Completable {
    println("onAccReplyFlow")
    //test
    var count = 0
    val d = Observable.interval(1, TimeUnit.SECONDS)
        .subscribe {
            println("${count / (it + 1)}/s")
        }
    return flow.onMessage
        .doOnSubscribe { flow.pull(200000) }
        .doOnNext { flow.pull(1) }
        .scan(0) { acc, _ -> acc + 1 }
        .doOnNext { count = it }
        .ignoreElements()
}


private fun crazyAcc(rootChannel: Channel): Completable {
    val header = Header.newBuilder().setInfoType("Acc")
    return rootChannel.sendHeader(header)
        .flatMapObservable { channel ->
            channel.onPull
                .flatMap { pull ->
                    Observable.just(ByteArray(1))
                        .repeat(pull.toLong())
                        .doOnNext { channel.messageSender.onNext(it) }
                }
        }
        .ignoreElements()
}