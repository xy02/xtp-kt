# xtp-kt

### 介绍
XTP协议的Kotlin实现（XTP是个极简的应用层通讯协议，详见[xtp.proto](src/main/proto/xtp.proto)）

### 特性
- 支持任意有序传输协议，TCP, WebSocket, QUIC等
- 支持背压的多路流
- 超高性能
- 易于构建微服务

### 安装教程
gradle:
```groovy
repositories {
    //...
    maven { url 'https://jitpack.io' }
}
dependencies {
    implementation "io.reactivex.rxjava3:rxjava:3.0.8"
    implementation 'com.google.protobuf:protobuf-javalite:3.14.0'
    implementation 'com.gitee.xy02:xtp-kt:0.11.0'
    //implementation 'com.github.xy02:xtp-kt:0.11.0'
}
```

### 使用说明
服务端：
```kotlin
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

private fun onClientRequester(requester: Requester): Completable {
    println("onClientRequester")
    //验证客户端，略
    //API列表
    val api = Observable.fromIterable(
        mutableListOf<API>(
            "Acc" to ::acc
        )
    )
    return requester.createResponseChannel(Response.newBuilder())
        .flatMapCompletable { channel ->
            api.flatMapCompletable { (type, fn) ->
                //发送API
                val req = Request.newBuilder().setType(type)
                channel.sendRequest(req, true)
//                        .flatMapCompletable { fn(it) }
                    .flatMapCompletable(fn)
            }
        }
        .onErrorComplete()
}

//累加收到的请求个数，响应json字符串，形如{"time":"2021-03-01 10:31:59","acc":13}
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
            flow?.pull(2000)
        }
        .ignoreElements()
}
```