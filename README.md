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
    implementation 'com.gitee.xy02:xtp-kt:0.9.0'
    //implementation 'com.github.xy02:xtp-kt:0.9.0'
}
```

### 使用说明
服务端：
```kotlin
fun main(args: Array<String>) {
    RxJavaPlugins.setErrorHandler { e -> println("RxJavaPlugins e:$e") }
    //创建TCP服务端Sockets
    nioServerSockets()
        .subscribeOn(Schedulers.newThread())//如果是安卓，需另起线程
        .flatMapSingle { socket ->
            println("onSocket")
            //转换Socket->Connection
            init(Header.newBuilder(), socket)
        }
        .subscribe(
            { conn ->
                println("onConnection")
                //业务函数
                acc(conn)
                //拉取才会收到请求（流头消息）
                conn.flow.messagePuller.onNext(10)
            },
            { err -> err.printStackTrace() },
        )
    readLine()
}

//累加收到的数据个数，并向下游流输出json字符串
// {"time":"2021-03-01 10:31:59","acc":13}
private fun acc(conn: Connection) {
    //订阅消息流
    conn.flow.getChildFlowByType("Acc")
        .flatMapSingle { flow ->
            //处理新流，验证请求，处理header.info等
            println("onHeader:${flow.header}\n")
            //创建下游流
            conn.channel
                .createChildChannel(
                    Header.newBuilder().setInfoType("AccReply")
                )
                .map { channel -> Pair(flow, channel) }
        }
        .flatMapCompletable { (accFlow, accReplyChannel) ->
            //处理上游发来的数据（未向上游拉取数据时是不会收到数据的）
            val df = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val handledData = accFlow.onMessage
                .scan(0) { acc, _ -> acc + 1 }
                .map { acc ->
                    val json = """{"time":${df.format(System.currentTimeMillis())},"acc":$acc}"""
                    json.toByteArray()
                }
            //向下游输出处理过的数据
            handledData.subscribe(accReplyChannel.messageSender)
            //自动流量控制
            accFlow.pipeChannels(
                //可以有多个下游管道
                mapOf(accReplyChannel to PipeSetup())
            )
        }
        .onErrorComplete()
        .subscribe()
}
```