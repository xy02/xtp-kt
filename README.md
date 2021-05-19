# xtp-kt

### 介绍
XTP协议的Kotlin实现（XTP是个极简的基于Protobuf的应用层通讯协议，详见[xtp.proto](src/main/proto/xtp.proto)）

### 特性
- 支持任意有序传输协议，TCP, WebSocket, QUIC等
- 支持反压(Backpressure)的多路流
- 使用响应式API限制数据源的生产，意味着不需要熔断器
- 高性能(E3-1231, 29%CPU, 156k 消息/秒)
- 易于构建微服务，可固定嵌套的上下文数据

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
    implementation 'com.gitee.xy02:xtp-kt:0.18.0'
    //implementation 'com.github.xy02:xtp-kt:0.18.0'
}
```

### 使用说明
服务端：
```kotlin
fun main() {
    RxJavaPlugins.setErrorHandler { e -> println("RxJavaPlugins e:$e") }
    //创建TCP服务端
    nioServer()
        .flatMapCompletable(::onPeer)
        .subscribe(
            { },
            { err -> err.printStackTrace() },
        )
    readLine()
}

private fun onPeer(peer: Peer): Completable {
    //声明根处理者信息
    val info = HandlerInfo.newBuilder()
        .setData(ByteString.copyFromUtf8("Service Info"))
        .setChannelSize(10)
    val rootHandler = peer.createHandler(info)
    val declare = Observable.just(rootHandler)
        .doOnNext(peer::declareRootHandlerInfo)
        .ignoreElements()
    //处理输入，这里是简单的路由逻辑
    val input = rootHandler.onData
        .flatMapCompletable {
            when(it.toStringUtf8()){
                "acc" -> onAcc(peer, rootHandler)
                else -> Completable.complete()
            }
        }
        .onErrorComplete()
    return Completable.merge(listOf(input, declare))
}

//服务的一个功能：累加收到的请求个数，响应json字符串，形如{"time":"2021-03-01 10:31:59","acc":13}
private fun onAcc(peer: Peer, rootHandler:Handler):Completable {
    //当收到"acc"请求时，产出（声明）"accHandler"处理者信息
    val info = HandlerInfo.newBuilder()
        .setData(ByteString.copyFromUtf8("accHandler"))
        .setChannelSize(10000)
    val handler = peer.createHandler(info)
    val declare = Observable.just(handler)
        .doOnNext(rootHandler::yieldHandler)
        .ignoreElements()
    val df = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //把接收的data产出（变换）为json字符串并输出
    val output = handler.onData
        .scan(0) { acc, _ -> acc + 1 }
        .map { acc ->
            val json = """{"time":${df.format(System.currentTimeMillis())},"acc":$acc}"""
            ByteString.copyFromUtf8(json)
        }
        .doOnNext(handler.yieldedDataSender::onNext)
        .ignoreElements()
    return Completable.merge(listOf(output, declare))
}
```

客户端：
```kotlin
fun main() {
    //创建TCP客户端
    nioClient(InetSocketAddress("localhost", 8001))
        .flatMap { it.onRootRemoteHandler }
        .flatMapCompletable(::onRootRemoteHandler)
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

private fun onRootRemoteHandler(rootRemoteHandler: RemoteHandler): Completable {
    println("onRootRemoteHandler")
    val output = Observable.just(ByteString.copyFromUtf8("acc"))
        .doOnNext(rootRemoteHandler.dataSender::onNext)
        .ignoreElements()
    val input = rootRemoteHandler.onRemoteHandler
        .flatMapCompletable { remoteHandler->
            when(remoteHandler.info.data.toStringUtf8()){
                "accHandler" -> onAccHandler(remoteHandler)
                else -> Completable.complete()
            }
        }
    return Completable.merge(listOf( input, output))
}

private fun onAccHandler(remoteHandler: RemoteHandler): Completable {
    println("onAccHandler")
    val output = Observable.merge(
        Observable.just(ByteString.copyFromUtf8("hi"))
            .repeat(remoteHandler.info.channelSize.toLong()),
        remoteHandler.onYieldedData,
    )
        .doOnNext(remoteHandler.dataSender::onNext)
        .ignoreElements()
    val input = remoteHandler.onYieldedData
        .scan(0) { acc, _ -> acc + 1 }
        .sample(1, TimeUnit.SECONDS)
        .scan(0) { acc, count ->
            //ops
            println("${count / (acc + 1)}/s")
            acc + 1
        }
        .ignoreElements()
    return Completable.merge(listOf( input, output))
}
```

### 传送门
XTP协议是跨语言且易于实现，任何支持Protobuf的语言都能实现
- Typescript实现(JS)：https://gitee.com/xy02/xtp-ts

### TODO
- TCP心跳