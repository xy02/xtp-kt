# xtp-kt

### 介绍
XTP协议的Kotlin实现（XTP是个极简的基于Protobuf的应用层通讯协议，详见[xtp.proto](src/main/proto/xtp.proto)）

### 特性
- 支持任意有序传输协议，TCP, WebSocket, QUIC等
- 支持反压(Backpressure)的多路流，意味着不需要熔断器
- 高性能(E3-1231, 12%CPU, 138k message/s)
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
    implementation 'com.gitee.xy02:xtp-kt:0.17.0'
    //implementation 'com.github.xy02:xtp-kt:0.17.0'
}
```

### 使用说明
服务端：
```kotlin
fun main(args: Array<String>) {
    RxJavaPlugins.setErrorHandler { e -> println("RxJavaPlugins e:$e") }
    //创建TCP服务端
    nioServer()
        .flatMapCompletable(::handleClientInfo)
        .subscribe(
            { },
            { err -> err.printStackTrace() },
        )
    readLine()
}

fun handleClientInfo(peer: Peer): Completable {
    println("handleClientInfo")
    return peer.singleRootFlow
        .flatMapCompletable { rootFlow->
            //验证收到流头数据rootFlow.header.text(或data)，略
            //发送根流头
            val header = Header.newBuilder().setText("ServiceInfo")
            peer.sendRootHeader(header)
                .flatMapCompletable { rootChannel->
                    service(rootFlow, rootChannel)
                }
        }
        .onErrorComplete()
}

fun service(rootFlow: Flow, rootChannel: Channel): Completable {
    println("onService, ${rootFlow.header}")
    return rootFlow.onChildFlow
        .doOnSubscribe {
            //拉取“流”
            rootChannel.onPull.subscribe(rootFlow.messagePuller)
        }
        .flatMapCompletable { flow ->
            when (flow.header.text) {
                "Acc" -> acc(flow, rootChannel)
                else -> Completable.complete()
            }
        }
}

//累加收到的请求个数，响应json字符串，形如{"time":"2021-03-01 10:31:59","acc":13}
private fun acc(accFlow: Flow, rootChannel: Channel): Completable {
    //处理新流，验证flow.header.text等
    println("onHeader:${accFlow.header}")
    //创建下游流
    return rootChannel
        .sendHeader(
            Header.newBuilder().setText("AccReply")
        )
        .flatMapCompletable { accReplyChannel ->
            //处理上游发来的数据（未向上游拉取数据时是不会收到数据的）
            val df = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val handledMessage = accFlow.onMessage
                .scan(0) { acc, _ -> acc + 1 }
                .map { acc ->
                    val json = """{"time":${df.format(System.currentTimeMillis())},"acc":$acc}"""
                    json.toByteArray()
                }
            //向下游输出处理过的消息
            handledMessage.subscribe(accReplyChannel.messageSender)
            //自动流量控制
            accFlow.pipeChannels(
                //可以有多个下游管道
                mapOf(accReplyChannel to PipeSetup())
            )
        }
}
```

###传送门
XTP协议是跨语言且易于实现，任何支持Protobuf的语言都能实现
- Typescript实现(JS)：https://gitee.com/xy02/xtp-ts

###TODO
- TCP心跳