package com.github.xy02.example

import com.github.xy02.xtp.*
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.schedulers.Schedulers
import xtp.Header
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

private fun main() {
    //创建TCP客户端
    nioClient(InetSocketAddress("localhost", 8001))
        .subscribeOn(Schedulers.newThread())
        .flatMapCompletable(::handlePeer)
        .repeat()
        .retryWhen { errors ->
            val counter = AtomicInteger()
            errors
                .takeWhile { e -> counter.getAndIncrement() != 3 }
                .flatMap { e ->
                    println("delay retry by " + counter.get() + " second(s)")
                    Flowable.timer(counter.get().toLong(), TimeUnit.SECONDS)
                }
        }
        .subscribe(
            { println("complete") },
            { err -> err.printStackTrace() },
        )
    readLine()
}

private fun handlePeer(peer: Peer): Completable {
    println("onPeer")
    //发送根流头
    val header = Header.newBuilder().setText("ClientInfo")
    return peer.sendRootHeader(header)
        .flatMapCompletable { rootChannel ->
            peer.singleRootFlow.flatMapCompletable { rootFlow ->
                onService(rootChannel, rootFlow)
            }
        }
}

private fun onService(rootChannel: Channel, rootFlow: Flow): Completable {
    println("onService, ${rootFlow.header}")
    return Completable.mergeArray(
        //输入
        rootFlow.onChildFlow
            .doOnSubscribe {
                //接收10条子流
                rootFlow.pull(10)
            }
            .take(1)
            .flatMapCompletable { flow ->
                when (flow.header.text) {
                    "AccReply" -> handleAccReply(flow)
                    else -> Completable.complete()
                }
            },
        //输出
        crazyAcc(rootChannel),
    ).onErrorComplete()
        .doOnComplete {
            println("onServiceReply doOnComplete")
            close(rootChannel.conn)
        }
}

private fun handleAccReply(flow: Flow): Completable {
    println("onAccReplyFlow")
    return flow.onMessage
        .doOnSubscribe { flow.pull(200000) }
        .doOnNext { flow.pull(1) }
        .scan(0) { acc, _ -> acc + 1 }
        .sample(1, TimeUnit.SECONDS)
        .scan(0) { acc, count ->
            //ops
            println("${count / (acc + 1)}/s")
            acc + 1
        }
        .doOnComplete { println("handleAccReply doOnComplete") }
        .ignoreElements()
}


private fun crazyAcc(rootChannel: Channel): Completable {
    val header = Header.newBuilder().setText("Acc")
    return rootChannel.sendHeader(header)
        .flatMapObservable { channel ->
            channel.onPull
                .flatMap { pull ->
                    Observable.just(ByteArray(1))
                        .repeat(pull.toLong())
                }
//                .take(30000)
                .doOnNext { channel.messageSender.onNext(it) }
                .doOnComplete { channel.messageSender.onComplete() }
        }
        .ignoreElements()
}