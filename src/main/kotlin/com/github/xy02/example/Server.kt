package com.github.xy02.example

import com.github.xy02.xtp.*
import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.plugins.RxJavaPlugins
import io.reactivex.rxjava3.schedulers.Schedulers
import xtp.Request
import xtp.Response
import java.text.SimpleDateFormat

typealias API = Pair<String, (Responder) -> Completable>

fun main(args: Array<String>) {
    RxJavaPlugins.setErrorHandler { e -> println("RxJavaPlugins e:$e") }
    //创建TCP服务端
    nioServer()
        .subscribeOn(Schedulers.newThread())//如果是安卓，需另起线程
        .flatMapSingle(Connection::onRootRequester)
        .flatMapCompletable(::onClientRequester)
        .subscribe(
            { println("complete") },
            { err -> err.printStackTrace() },
        )
    readLine()
}

private fun onClientRequester(requester:Requester):Completable {
    println("onClientRequester")
    //验证客户端，略
    //准备API列表
    val apiList = mutableListOf<API>(
        "Acc" to ::acc
    )
    return requester.createResponseChannel(Response.newBuilder())
        .flatMapCompletable { channel->
            channel.onPull.flatMap { pull ->
                Observable.generate<API> { emitter ->
                    if (apiList.isEmpty())
                        return@generate emitter.onComplete()
                    val api = apiList.removeLast()
                    emitter.onNext(api)
                }.take(pull.toLong())
            }.flatMapCompletable { (type, fn) ->
                //返回API
                val req = Request.newBuilder().setType(type)
                channel.sendRequest(req)
//                    .flatMapCompletable { fn(it) }
                    .flatMapCompletable(fn)
            }.onErrorComplete()
        }
}

//累加收到的数据个数，并向下游流输出json字符串
// {"time":"2021-03-01 10:31:59","acc":13}
private fun acc(responder: Responder): Completable {
    //父流，接收的是请求
    val flow = responder.flow
    val onRequester = flow?.onRequester ?: Observable.empty()
    val df = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //处理请求消息（未向父流拉取数据时不会收到消息）
    return onRequester
        .map { Pair(it, 1) }
        .scan { (_, acc), (req, n) -> Pair(req, acc + n) }
        .map { (req, acc) ->
            val json = """{"time":${df.format(System.currentTimeMillis())},"acc":$acc}"""
            val bytes = json.toByteArray()
//                val json = io.vertx.core.json.JsonObject()
//                    .put("time", df.format(System.currentTimeMillis()))
//                    .put("acc", acc)
//                json.toBuffer().bytes
//                val bb = java.nio.ByteBuffer.allocate(4)
//                bb.putInt(acc)
//                bb.array()
            Pair(req, bytes)
        }
        .doOnNext { (req, bytes) ->
            //应答
            val res = Response.newBuilder().setData(ByteString.copyFrom(bytes)).build()
            req.reply(res)
            //拉取
            flow?.pull(1)
        }
        .doOnSubscribe {
            //首次拉取
            flow?.pull(5000)
        }
        .ignoreElements()
}
