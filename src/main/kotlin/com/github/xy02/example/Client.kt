package com.github.xy02.example

import com.github.xy02.xtp.Responder
import com.github.xy02.xtp.nioClient
import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.schedulers.Schedulers
import xtp.Request
import xtp.Success
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
            //向服务器获取API，可以带上身份信息等数据
            val req = Request.newBuilder().setDataClass("FetchAPI")
            conn.sendRootRequest(req)
        }
        .flatMapObservable { requester ->
            println("onServiceRequester")
            requester.flow?.onResponder
                ?.doOnSubscribe {
                    //接收10个API
                    requester.flow.pull(10)
                }
                ?: Observable.error(Exception("The service does not provide any API"))
        }
        .flatMapCompletable { responder ->
            println("onAPI type:${responder.request.dataClass}")
            when (responder.request.dataClass) {
                "Acc" -> crazyAcc(responder)
                else -> Completable.complete()
            }
        }
        .subscribe(
            { println("complete") },
            { err -> err.printStackTrace() },
        )
    readLine()
}

private fun crazyAcc(responder: Responder): Completable {
    return responder.createResponseChannel(Success.newBuilder())
        .flatMapCompletable { channel ->
            val onRes = channel.onPull.flatMap { pull ->
                Observable.just(ByteArray(1))
                    .repeat(pull.toLong())
                    .flatMapSingle { data ->
                        val req = Request.newBuilder().setData(ByteString.copyFrom(data))
                        channel.sendRequest(req)
                    }
                    .map { it.response }
            }
            //test
            var count = 0
            val d = Observable.interval(1, TimeUnit.SECONDS)
                .subscribe {
                    println("${count / (it + 1)}/s")
                }
            onRes.scan(0) { acc, _ -> acc + 1 }
                .doOnNext { count = it }
                .ignoreElements()
        }
}

private fun intervalAcc(responder: Responder): Completable {
    return responder.createResponseChannel(Success.newBuilder())
        .flatMapCompletable { channel ->
            Observable.interval(1, TimeUnit.SECONDS)
                .flatMapSingle {
                    channel.sendRequest(Request.newBuilder())
                }
                .doOnNext { requester ->
                    println("response: ${requester.response.success.data.toStringUtf8()}")
                }
                .onErrorComplete()
                .ignoreElements()
        }

}