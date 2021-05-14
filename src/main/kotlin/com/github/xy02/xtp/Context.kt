package com.github.xy02.xtp

import com.github.xy02.rx.getSubValues
import io.reactivex.rxjava3.core.Observable
import xtp.Frame
import java.util.concurrent.atomic.AtomicInteger

//私有的上下文
internal data class Context(
    val conn: Connection,
    val watchMessageFrames: (flowId: Int) -> Observable<Frame>,
    val watchCloseFrames: (flowId: Int) -> Observable<Frame>,
    val watchYieldFrames: (flowId: Int) -> Observable<Frame>,
    val watchEndFrames: (flowId: Int) -> Observable<Frame>,
    val newHandlerId: () -> Int,
)

internal fun newContext(conn: Connection): Context {
    val getFramesByType = conn.onFrame.getSubValues { frame -> frame.typeCase }
    val messageFrames = getFramesByType(Frame.TypeCase.MESSAGE)
    val closeFrames = getFramesByType(Frame.TypeCase.CLOSE)
    val yieldFrames = getFramesByType(Frame.TypeCase.YIELD)
    val endFrames = getFramesByType(Frame.TypeCase.END)
    val watchMessageFrames = messageFrames.getSubValues(Frame::getHandlerId)
    val watchCloseFrames = closeFrames.getSubValues(Frame::getHandlerId)
    val watchYieldFrames = yieldFrames.getSubValues(Frame::getHandlerId)
    val watchEndFrames = endFrames.getSubValues(Frame::getHandlerId)
    val hid = AtomicInteger(1)
    val newHandlerId = hid::getAndIncrement
    return Context(
        conn = conn,
        watchMessageFrames = watchMessageFrames,
        watchCloseFrames = watchCloseFrames,
        watchYieldFrames = watchYieldFrames,
        watchEndFrames = watchEndFrames,
        newHandlerId = newHandlerId,
    )
}