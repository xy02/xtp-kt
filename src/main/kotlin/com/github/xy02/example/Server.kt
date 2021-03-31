package com.github.xy02.example

import com.github.xy02.xtp.Connection
import com.github.xy02.xtp.Responder
import com.github.xy02.xtp.Requester
import com.github.xy02.xtp.nioServer
import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.plugins.RxJavaPlugins
import xtp.Request
import xtp.Response
import java.text.SimpleDateFormat

private typealias API = Pair<String, (Requester) -> Completable>

fun main(args: Array<String>) {
    RxJavaPlugins.setErrorHandler { e -> println("RxJavaPlugins e:$e") }
    //创建TCP服务端
    nioServer()
        .flatMapSingle(Connection::onRootResponder)
        .flatMapCompletable(::onClientResponder)
        .subscribe(
            { println("complete") },
            { err -> err.printStackTrace() },
        )
    readLine()
}

private fun onClientResponder(responder: Responder): Completable {
    println("onClientResponder")
    //验证客户端，略
    //API列表
    val api = Observable.fromIterable(
        listOf<API>(
            "Acc" to ::acc
        )
    )
    return responder.createResponseChannel(Response.newBuilder())
        .flatMapCompletable { channel ->
            api.flatMapCompletable { (type, fn) ->
                //发送API
                val req = Request.newBuilder().setDataClass(type)
                channel.sendRequest(req, true)
//                        .flatMapCompletable { fn(it) }
                    .flatMapCompletable(fn)
            }
        }
        .onErrorComplete()
}

//累加收到的请求个数，响应json字符串，形如{"time":"2021-03-01 10:31:59","acc":13}
private fun acc(requester: Requester): Completable {
    //消息流，接收的是“请求”消息
    val flow = requester.flow
    //“请求”消息的响应器
    val onResponder = flow?.onResponder ?: Observable.empty()
    val df = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //处理“请求”消息（未向消息流拉取数据时不会收到消息）
    return onResponder
        .map { Pair(it, 1) }
        .scan { (_, acc), (responder, n) -> Pair(responder, acc + n) }
        .map { (responder, acc) ->
            val json = """{"time":${df.format(System.currentTimeMillis())},"acc":$acc}"""
            val bytes = json.toByteArray()
//                val json = io.vertx.core.json.JsonObject()
//                    .put("time", df.format(System.currentTimeMillis()))
//                    .put("acc", acc)
//                json.toBuffer().bytes
//                val bb = java.nio.ByteBuffer.allocate(4)
//                bb.putInt(acc)
//                bb.array()
            Pair(responder, bytes)
        }
        .doOnNext { (responder, bytes) ->
            //应答
            val res = Response.newBuilder().setData(ByteString.copyFrom(bytes)).build()
            responder.reply(res)
            //拉取“请求”消息
            flow?.pull(1)
        }
        .doOnSubscribe {
            //首次拉取
            flow?.pull(2000)
        }
        .ignoreElements()
}
