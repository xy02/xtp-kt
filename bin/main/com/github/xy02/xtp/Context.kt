package com.github.xy02.xtp

import com.github.xy02.rx.getSubValues
import io.reactivex.rxjava3.core.Observable
import xtp.Frame
import java.util.concurrent.atomic.AtomicInteger

//私有的上下文
internal data class Context(
    val conn: Connection,
    val watchCancelFrames: (flowId: Int) -> Observable<Frame>,
    val watchPullFrames: (flowId: Int) -> Observable<Frame>,
    val watchEndFrames: (flowId: Int) -> Observable<Frame>,
    val watchMessageFrames: (flowId: Int) -> Observable<Frame>,
    val newFlowId: () -> Int,
)

internal fun newContext(conn: Connection): Context {
    val (onFrame, _) = conn
    val getFramesByType = onFrame.getSubValues { frame -> frame.typeCase }
    val messageFrames = getFramesByType(Frame.TypeCase.MESSAGE)
    val endFrames = getFramesByType(Frame.TypeCase.END)
    val pullFrames = getFramesByType(Frame.TypeCase.PULL)
    val cancelFrames = getFramesByType(Frame.TypeCase.CANCEL)
    val watchMessageFrames = messageFrames.getSubValues(Frame::getFlowId)
    val watchEndFrames = endFrames.getSubValues(Frame::getFlowId)
    val watchPullFrames = pullFrames.getSubValues(Frame::getFlowId)
    val watchCancelFrames = cancelFrames.getSubValues(Frame::getFlowId)
    val fid = AtomicInteger(1)
    val newFlowId = fid::getAndIncrement
    return Context(
        conn = conn,
        watchCancelFrames = watchCancelFrames,
        watchPullFrames = watchPullFrames,
        watchEndFrames = watchEndFrames,
        watchMessageFrames = watchMessageFrames,
        newFlowId = newFlowId,
    )
}