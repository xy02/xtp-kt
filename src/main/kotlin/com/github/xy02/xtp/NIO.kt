package com.github.xy02.xtp

import com.github.xy02.rx.getSubValues
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.ObservableEmitter
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.PublishSubject
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.util.concurrent.atomic.AtomicInteger

data class TCPServerOptions(
    val address: SocketAddress = InetSocketAddress(8001),
    val source: SocketsSource = nioSocketsSource(),
)

data class TCPClientOptions(
    val source: SocketsSource = nioSocketsSource(),
)

data class SocketsSource(
    val selector: Selector,
    val newGroupId: () -> Int,
    val getSocketContextByGroupId: (Int) -> Observable<SocketContext>,
)

data class SocketContext(
    val groupId: Int,
    val theSocket: Single<Socket>,
)

internal data class SocketChannelAttachment(
    val emitter: ObservableEmitter<ByteArray>,
    val bufOfLength: ByteBuffer = ByteBuffer.allocate(4).put(0),
    var bufOfBody: ByteBuffer? = null,
)

fun nioSocketsSource(): SocketsSource {
    val selector = Selector.open()
    val sockets = Observable.create<SocketContext> {
        println("new NIO selector on ${Thread.currentThread().name} : ${Thread.currentThread().id}")
        while (selector.isOpen) {
            selector.select()
            val ite = selector.selectedKeys().iterator()
            while (ite.hasNext()) {
                val key = ite.next()
                ite.remove()
                if (!key.isValid) continue
                when (key.readyOps()) {
                    SelectionKey.OP_ACCEPT -> {
                        val ssc = key.channel() as ServerSocketChannel
                        val groupId = key.attachment() as Int
//                        println("OP_ACCEPT: localPort=${ssc.socket().localPort}, groupId=$groupId")
                        val theSocket = try {
                            val sc = ssc.accept()
//                            println("sc: port=${sc.socket().port}, localPort=${sc.socket().localPort}")
                            sc.configureBlocking(false)
                            val socket = newSocketFromSocketChannel(sc, key.selector())
                            Single.just(socket)
                        } catch (ex: Exception) {
                            Single.error(ex)
                        }
                        it.onNext(SocketContext(groupId, theSocket))
                    }
                    SelectionKey.OP_CONNECT -> {
                        val sc = key.channel() as SocketChannel
                        val groupId = key.attachment() as Int
//                        println("OP_CONNECT: localPort=${sc.socket().localPort}, groupId=$groupId")
                        val theSocket = try {
                            sc.finishConnect()
//                            println("sc: port=${sc.socket().port}, localPort=${sc.socket().localPort}")
                            val socket = newSocketFromSocketChannel(sc, key.selector())
                            Single.just(socket)
                        } catch (ex: Exception) {
                            println("finishConnect: $ex")
                            sc.close()
                            key.cancel()
                            Single.error(ex)
                        }
                        it.onNext(SocketContext(groupId, theSocket))
                    }
                    SelectionKey.OP_READ -> {
                        val sc = key.channel() as SocketChannel
                        val att = key.attachment() as SocketChannelAttachment
//                        println("sc: ${sc}, att:${att}")
                        var loop = true
                        while (loop) {
                            loop = readSocketChannel(sc, att)
                        }
                    }
                }
            }
        }
//        println("selector closed tid : ${Thread.currentThread().id}")
        it.onComplete()
    }.subscribeOn(Schedulers.io())
    val getSocketContextByGroupId = getSubValues(sockets) { it.groupId }
    val gid = AtomicInteger()
    val newGroupId = { gid.getAndIncrement() }
    return SocketsSource(selector, newGroupId, getSocketContextByGroupId)
}

fun nioServerSockets(
    options: TCPServerOptions = TCPServerOptions(),
): Observable<Socket> {
    val source = options.source
    val gid = source.newGroupId()
    return source.getSocketContextByGroupId(gid)
        .doOnSubscribe {
            val ssc = ServerSocketChannel.open()
            ssc.configureBlocking(false)
            ssc.register(source.selector, SelectionKey.OP_ACCEPT, gid)
            ssc.socket().bind(options.address)
        }
        .subscribeOn(Schedulers.io())
        .flatMapMaybe { cx ->
            cx.theSocket
                .doOnError { ex -> ex.printStackTrace() }
                .onErrorComplete()
        }
}

fun nioClientSocket(
    address: SocketAddress,
    options: TCPClientOptions = TCPClientOptions(),
): Single<Socket> {
    val source = options.source
    return Single.fromCallable { source.newGroupId() }
        .flatMap { gid ->
//            println("gid $gid")
            source.getSocketContextByGroupId(gid)
                .doOnSubscribe {
                    val sc = SocketChannel.open()
                    sc.configureBlocking(false)
                    sc.register(source.selector, SelectionKey.OP_CONNECT, gid)
                    sc.connect(address)
                    source.selector.wakeup()
//                    println("connect")
                }
                .subscribeOn(Schedulers.io())
                .take(1)
                .singleOrError()
                .flatMap { cx -> cx.theSocket }
        }
}


internal fun newSocketFromSocketChannel(sc: SocketChannel, selector: Selector): Socket {
    val sender = PublishSubject.create<ByteArray>()
    val buffers = Observable.create<ByteArray> { emitter1 ->
        sc.register(selector, SelectionKey.OP_READ, SocketChannelAttachment(emitter1))
        emitter1.setDisposable(Disposable.fromAction { sender.onComplete() })
    }
//        .observeOn(Schedulers.computation())
        .share()
    sender
//        .observeOn(Schedulers.io())
        .subscribe(
            { buf ->
                try {
                    val size = buf.size
                    val lengthBuf = ByteBuffer.allocate(3)
                        .put((size shr 16 and 0xff).toByte())
                        .put((size shr 8 and 0xff).toByte())
                        .put((size and 0xff).toByte())
                    lengthBuf.flip()
                    while (lengthBuf.hasRemaining()) sc.write(lengthBuf)
                    val bodyBuf = ByteBuffer.wrap(buf)
                    while (bodyBuf.hasRemaining()) sc.write(bodyBuf)
//                    println("write $bodyBuf on ${Thread.currentThread().name} : ${Thread.currentThread().id}")
                } catch (e: Exception) {
                    println("write SocketChannel: $e")
                    sc.close()
                }
            },
            { err ->
                println("sender onError:$err")
                sc.close()
            },
            {
                println("sender onComplete")
                sc.close()
            }
        )
    return Socket(buffers, sender)
}

internal fun readSocketChannel(sc: SocketChannel, att: SocketChannelAttachment): Boolean {
    val buf = att.bufOfBody ?: att.bufOfLength
    try {
        val bytesRead = sc.read(buf)
        if (bytesRead == -1) {
            sc.close()
            if (!att.emitter.isDisposed) att.emitter.tryOnError(java.nio.channels.ClosedChannelException())
            return false
        }
    } catch (e: Exception) {
        println("read SocketChannel: $e")
        if (!att.emitter.isDisposed) att.emitter.tryOnError(e)
    }
    if (buf.hasRemaining()) return false
    if (att.bufOfBody == null) {
        //从bufOfLength获取body长度
        buf.flip()
        att.bufOfBody = ByteBuffer.allocate(buf.int)
        buf.clear()
        buf.put(0)//body长度实际只有3字节，这里把最高位字节设置为0
    } else {
//        println("read $buf on ${Thread.currentThread().name} : ${Thread.currentThread().id}")
        att.emitter.onNext(buf.array())
        att.bufOfBody = null
    }
//    readSocketChannel(sc, att)
    return true
}
