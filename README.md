# xtp-kt

### 介绍
XTP协议的Kotlin实现（XTP是个极简的应用层通讯协议，详见[xtp.proto](src/main/proto/xtp.proto)）

### 特性
- 支持任意有序传输协议，TCP, WebSocket, QUIC等
- 支持背压的多路流
- 高性能
- 易于构建微服务

### 安装教程
gradle:
```groovy
repositories {
    //...
    maven { url 'https://jitpack.io' }
}
dependencies {
    implementation "io.reactivex.rxjava3:rxjava:3.0.11"
    implementation 'com.google.protobuf:protobuf-javalite:3.14.0'
    implementation 'com.gitee.xy02:xtp-kt:0.15.0'
    //implementation 'com.github.xy02:xtp-kt:0.15.0'
}
```

### 使用说明
服务端：
```kotlin
private typealias API = Pair<String, (Requester) -> Completable>

fun main(args: Array<String>) {
    RxJavaPlugins.setErrorHandler { e -> println("RxJavaPlugins e:$e") }
    //创建TCP服务端
    nioServer()
        .flatMapSingle(Connection::onRootProvider)
        .flatMapCompletable(::onClientProvider)
        .subscribe(
            { println("complete") },
            { err -> err.printStackTrace() },
        )
    readLine()
}

private fun onClientProvider(provider: Provider): Completable {
    println("onClientProvider")
    //验证客户端，略
    //API列表
    val api = Observable.fromIterable(
        listOf<API>(
            "Acc" to ::acc
        )
    )
    return provider.createResponseChannel(Success.newBuilder())
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
private fun acc(consumer: Consumer): Completable {
    //消息流，接收的是“请求”消息
    val flow = consumer.flow
    //“请求”消息的响应器
    val onProvider = flow?.onProvider ?: Observable.empty()
    val df = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //处理“请求”消息（未向消息流拉取数据时不会收到消息）
    return onProvider
        .map { Pair(it, 1) }
        .scan { (_, acc), (provider, n) -> Pair(provider, acc + n) }
        .map { (provider, acc) ->
            val json = """{"time":${df.format(System.currentTimeMillis())},"acc":$acc}"""
            val bytes = json.toByteArray()
            Pair(provider, bytes)
        }
        .doOnNext { (provider, bytes) ->
            //应答
            val success = Success.newBuilder().setData(ByteString.copyFrom(bytes))
            provider.replySuccess(success)
            //拉取“请求”消息
            flow?.pull(1)
        }
        .doOnSubscribe {
            //首次拉取
            flow?.pull(2000)
        }
        .ignoreElements()
}
```