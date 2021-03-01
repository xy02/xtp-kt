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
    implementation 'com.gitee.xy02:xtp-kt:0.3.1'
}
```

### 使用说明
服务端：
```kotlin
fun main(args: Array<String>) {
    RxJavaPlugins.setErrorHandler { e -> println("RxJavaPlugins e:$e") }
    //创建初始化函数
    val init = initWith(InfoHeader(
        //可包含自身身份证明等信息
        peerInfo = PeerInfo.getDefaultInstance(),
        //注册可接收的handlerName
        register = mapOf("acc" to Accept.getDefaultInstance())
    ))
    //创建TCP客户端Socket
    //nioClientSocket(InetSocketAddress("localhost", 8001))
    //创建TCP服务端Sockets
    nioServerSockets()
        .subscribeOn(Schedulers.newThread())//如果是安卓，需另起线程
        .flatMapMaybe { socket ->
            //转换Socket->Connection
            init(socket)
                .doOnError { err -> println("init err:$err") }
                .onErrorComplete()
        }
        .subscribe(
            { conn ->
                //业务函数
                acc(conn)
            },
            { err -> err.printStackTrace() },
        )
    readLine()
}

//累加收到的数据个数，并向下游流输出json字符串
// {"time":"2021-03-01 10:31:59","acc":13}
private fun acc(conn: Connection) {
    //获取消息流
    val onStream = conn.getStreamByType("acc")
    onStream.onErrorComplete().subscribe { stream ->
        //验证请求，处理header.data等
        println("onHeader:${stream.header}\n")
        val df = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        //处理上游发来的数据（未向上游拉取数据时是不会收到数据的）
        val handledData = stream.onMessage
            .scan(0) { acc, _ -> acc + 1 }
            .map { acc ->
                val json = """{"time":${df.format(System.currentTimeMillis())},"acc":$acc}"""
                json.toByteArray()
            }
        //创建下游流（会发送header）
        val accReplyChannel = stream.createChannel(Header.newBuilder().setHandlerName("accReply"))
        //向下游输出，自动流量控制
        stream.pipeChannels(
            PipeConfig(
                mapOf(accReplyChannel to handledData)
            )
        )
    }
}
```