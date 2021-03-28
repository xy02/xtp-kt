package com.github.xy02.rx

import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.ObservableEmitter
import io.reactivex.rxjava3.disposables.Disposable
import java.util.*
import java.util.concurrent.ConcurrentHashMap

//获取子数据流
fun <K, V> Observable<V>.getSubValues(keySelector: (V) -> K): (K) -> Observable<V> {
    val m = ConcurrentHashMap<K, MutableSet<ObservableEmitter<V>>>()
//    val m = mutableMapOf<K,MutableSet<ObservableEmitter<V>>>()
    val theEnd = this
        .doOnNext { v: V ->
            val key = keySelector(v)
            m[key]?.forEach { emitter -> emitter.onNext(v) }
        }
        .doOnError { e ->
            m.forEach { entry ->
                entry.value.forEach { emitter -> if (!emitter.isDisposed) emitter.tryOnError(e) }
            }
        }
        .doOnComplete {
            m.forEach { entry ->
                entry.value.forEach { emitter -> if (!emitter.isDisposed) emitter.onComplete() }
            }
        }
        .onErrorComplete()
        .ignoreElements()
        .cache()
    return { key ->
        Observable.create { emitter ->
//            println("key:$key")
            val emitterSet = m.getOrPut(key) {
                Collections.newSetFromMap(ConcurrentHashMap())
//                mutableSetOf()
            }
            emitterSet.add(emitter)
            val d = theEnd.subscribe()
            emitter.setDisposable(Disposable.fromAction {
                emitterSet.remove(emitter)
                if (emitterSet.size == 0) {
                    m.remove(key)
                }
                d.dispose()
            })
        }
    }
}