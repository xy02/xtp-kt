package com.github.xy02.rx

import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.subjects.PublishSubject
import io.reactivex.rxjava3.subjects.Subject
import java.util.concurrent.ConcurrentHashMap

//获取子数据流
fun <K, V> Observable<V>.getSubValues(keySelector: (V) -> K): (K) -> Observable<V> {
    val m = ConcurrentHashMap<K, Subject<V>>()
    val theEnd = this
        .doOnNext { v ->
            val key = keySelector(v)
            val subject = m[key]
            subject?.onNext(v)
        }
        .lastElement()
        .toObservable()
        .share()
    return { key ->
        if (key == null) Observable.error(Exception("key must not null"))
        else {
            val subject = m.getOrPut(key) {
                PublishSubject.create<V>().toSerialized()
            }
            subject.takeUntil(theEnd)
                .doFinally {
                    if (!subject.hasObservers())
                        m.remove(key)
                }
        }
    }
}