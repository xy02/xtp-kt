# xtp-kt

### 介绍
XTP协议的Kotlin实现（XTP是个极简的应用层通讯协议，详见[xtp.proto](src/main/proto/xtp.proto)）

### 特性
- 支持任意有序传输协议，TCP, WebSocket, QUIC等
- 支持回压(Backpressure)的多路流
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
    implementation 'com.gitee.xy02:xtp-kt:0.16.1'
    //implementation 'com.github.xy02:xtp-kt:0.16.0'
}
```

### 使用说明
服务端：
```kotlin
fun main(args: Array<String>) {
    RxJavaPlugins.setErrorHandler { e -> println("RxJavaPlugins e:$e") }
    //创建TCP服务端
    nioServer()
        .flatMapSingle { conn ->
            println("onConnection")
            conn.singleRootFlow
        }
        .flatMapCompletable(::handleClientInfo)
        .subscribe(
            { },
            { err -> err.printStackTrace() },
        )
    readLine()
}

fun handleClientInfo(rootFlow: Flow): Completable {
    println("handleClientInfo")
    //验证收到的header.info，略
    val conn = rootFlow.conn
    //发送根流头
    val header = Header.newBuilder().setInfoType("ServiceInfo")
    return conn.sendRootHeader(header)
        .flatMapCompletable { rootChannel ->
            service(rootFlow, rootChannel)
        }
        .onErrorComplete()
}

fun service(rootFlow: Flow, rootChannel: Channel): Completable {
    println("onService")
    return rootFlow.onChildFlow
        .doOnSubscribe {
            //拉取“流”
            rootChannel.onPull.subscribe(rootFlow.messagePuller)
        }
        .flatMapCompletable { flow ->
            when (flow.header.infoType) {
                "Acc" -> acc(flow, rootChannel)
                else -> Completable.complete()
            }
        }
}

//累加收到的请求个数，响应json字符串，形如{"time":"2021-03-01 10:31:59","acc":13}
private fun acc(flow: Flow, rootChannel: Channel): Completable {
    //处理新流，验证header.info等
    println("onHeader:${flow.header}")
    //创建下游流
    return rootChannel
        .sendHeader(
            Header.newBuilder().setInfoType("AccReply")
        )
        .flatMapCompletable { accReplyChannel ->
            //处理上游发来的数据（未向上游拉取数据时是不会收到数据的）
            val df = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val handledMessage = flow.onMessage
                .scan(0) { acc, _ -> acc + 1 }
                .map { acc ->
                    val json = """{"time":${df.format(System.currentTimeMillis())},"acc":$acc}"""
                    json.toByteArray()
                }
            //向下游输出处理过的消息
            handledMessage.subscribe(accReplyChannel.messageSender)
            //自动流量控制
            flow.pipeChannels(
                //可以有多个下游管道
                mapOf(accReplyChannel to PipeSetup())
            )
        }
}
```