package com.github.xy02.xtp

import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.PublishSubject
import io.reactivex.rxjava3.subjects.Subject
import xtp.Frame
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel

data class TCPServerOptions(
    val address: SocketAddress = InetSocketAddress(8001),
//    val source: SocketsSource = nioSocketsSource(),
)

internal data class SocketChannelAttachment(
    val emitter: Subject<ByteArray>,
    val bufOfLength: ByteBuffer = ByteBuffer.allocate(4).put(0),
    var bufOfBody: ByteBuffer? = null,
)

fun nioServer(
    options: TCPServerOptions = TCPServerOptions(),
): Observable<Peer> {
    return Observable.create<Connection> { emitter ->
        val selector = Selector.open()
        println("new nioServer selector on ${Thread.currentThread().name} : ${Thread.currentThread().id}")
        val ssc = ServerSocketChannel.open()
        ssc.configureBlocking(false)
        ssc.register(selector, SelectionKey.OP_ACCEPT)
        ssc.socket().bind(options.address)
        while (selector.isOpen) {
            selector.select()
            val ite = selector.selectedKeys().iterator()
            while (ite.hasNext()) {
                val key = ite.next()
                ite.remove()
                if (!key.isValid) continue
                when (key.readyOps()) {
                    SelectionKey.OP_ACCEPT -> {
//                        val ssc = key.channel() as ServerSocketChannel
//                        println("OP_ACCEPT: localPort=${ssc.socket().localPort}, groupId=$groupId")
                        try {
                            val sc = ssc.accept()
//                            println("sc: port=${sc.socket().port}, localPort=${sc.socket().localPort}")
                            sc.configureBlocking(false)
                            val conn = newConnectionFromSocketChannel(sc, key.selector())
                            emitter.onNext(conn)
                        } catch (ex: Exception) {
                            println("ServerSocketChannel accept: $ex")
                            key.cancel()
                        }
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
        emitter.onComplete()
    }
        .subscribeOn(Schedulers.io())
//        .observeOn(Schedulers.computation())
        .map(::toPeer)
}

fun nioClient(
    address: SocketAddress,
): Single<Peer> {
    return Single.create<Connection> { emitter ->
        val selector = Selector.open()
        println("new nioClient selector on ${Thread.currentThread().name} : ${Thread.currentThread().id}")
        val sc = SocketChannel.open()
        sc.configureBlocking(false)
        sc.register(selector, SelectionKey.OP_CONNECT)
        sc.connect(address)
        while (selector.isOpen) {
            selector.select()
            val ite = selector.selectedKeys().iterator()
            while (ite.hasNext()) {
                val key = ite.next()
                ite.remove()
                if (!key.isValid) continue
                when (key.readyOps()) {
                    SelectionKey.OP_CONNECT -> {
//                        val sc = key.channel() as SocketChannel
//                        println("OP_CONNECT: localPort=${sc.socket().localPort}, groupId=$groupId")
                        try {
                            sc.finishConnect()
//                            println("sc: port=${sc.socket().port}, localPort=${sc.socket().localPort}")
                            val conn = newConnectionFromSocketChannel(sc, key.selector(), true)
                            emitter.onSuccess(conn)
                        } catch (ex: Exception) {
                            println("SocketChannel finishConnect: $ex")
                            key.cancel()
                            selector.close()
                            emitter.onError(ex)
                        }
                    }
                    SelectionKey.OP_READ -> {
//                        val sc = key.channel() as SocketChannel
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
//        println("selector close")
    }
        .subscribeOn(Schedulers.io())
//        .observeOn(Schedulers.computation())
        .map(::toPeer)
}


internal fun newConnectionFromSocketChannel(
    sc: SocketChannel,
    selector: Selector,
    closeSelector: Boolean = false
): Connection {
    val sender = PublishSubject.create<Frame>().toSerialized()
    val onBytes = PublishSubject.create<ByteArray>().toSerialized()
    sc.register(selector, SelectionKey.OP_READ, SocketChannelAttachment(onBytes))
    val onFrame = onBytes
        .observeOn(Schedulers.computation())
        .takeUntil(sender.lastElement().toObservable())
        .map(Frame::parseFrom)
//        .doOnNext {
//            println("onFrame $it")
//        }
        .doOnTerminate {
            sc.close()
            if (closeSelector) selector.close()
        }
        .share()
    sender
//        .doOnNext {
//            println("onSend $it")
//        }
        .takeUntil(sender.lastElement().toObservable())
        .map(Frame::toByteArray)
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
//    return Connection(onFrame, sender)
    return object :Connection{
        override val onFrame: Observable<Frame>
            get() = onFrame
        override val frameSender: Observer<Frame>
            get() = sender
    }
}

internal fun readSocketChannel(sc: SocketChannel, att: SocketChannelAttachment): Boolean {
    val buf = att.bufOfBody ?: att.bufOfLength
    val emitter = att.emitter
    try {
        val bytesRead = sc.read(buf)
        if (bytesRead == -1) {
            println("read -1")
            emitter.onComplete()
            sc.close()
            return false
        }
    } catch (e: Exception) {
        println("read SocketChannel: $e")
        emitter.onError(e)
        return false
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
        emitter.onNext(buf.array())
        att.bufOfBody = null
    }
//    readSocketChannel(sc, att)
    return true
}
