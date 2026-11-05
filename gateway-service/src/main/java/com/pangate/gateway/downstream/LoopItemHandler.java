package com.pangate.gateway.downstream;

import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.PushItem;
import com.pangate.gateway.loop.LoopContext;
import io.netty.channel.Channel;

/** What a {@link LoopBatchTask} does with each item on its loop (spec §6.3). */
public interface LoopItemHandler {

  /** Unicast / RPC_REQ; returns the channel written to (flushed at the end of the task) or null. */
  Channel message(LoopContext loop, PushItem item, StreamRef stream);

  /** Takes one share of {@code frame}. */
  void broadcast(LoopContext loop, String topic, PushItem.Priority priority, PreEncodedFrame frame);

  void control(LoopContext loop, String principal, ControlOp op);

  /** Production wiring: delivery, the loop's pacer, control push. */
  static LoopItemHandler of(PushDelivery delivery, ControlPushHandler control) {
    return new LoopItemHandler() {
      @Override
      public Channel message(LoopContext loop, PushItem item, StreamRef stream) {
        return delivery.deliver(loop, item, stream);
      }

      @Override
      public void broadcast(LoopContext loop, String topic, PushItem.Priority priority, PreEncodedFrame frame) {
        loop.pacer().submit(topic, priority, frame);
      }

      @Override
      public void control(LoopContext loop, String principal, ControlOp op) {
        control.apply(loop, principal, op);
      }
    };
  }
}
