package com.pangate.gateway.downstream;

import com.pangate.contracts.v1.PushItem;
import com.pangate.gateway.loop.LoopContext;
import io.netty.channel.Channel;
import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;
import java.util.ArrayList;
import java.util.List;

/** The items of one batch for one event loop: handled in order on that loop, then each written channel flushed once. */
final class LoopBatchTask implements Runnable {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(LoopBatchTask.class);

  private final LoopContext loop;
  private final LoopItemHandler handler;
  private final StreamRef stream;
  private final List<PushItem> items = new ArrayList<>();
  private final List<PreEncodedFrame> frames = new ArrayList<>();
  private final io.micrometer.core.instrument.Counter errors;

  LoopBatchTask(LoopContext loop, LoopItemHandler handler, StreamRef stream,
      io.micrometer.core.instrument.Counter errors) {
    this.loop = loop;
    this.handler = handler;
    this.stream = stream;
    this.errors = errors;
  }

  /** The loop no longer accepts tasks (shutdown): reliable messages are NACK_CLOSED, broadcast shares released. */
  void rejected() {
    for (int i = 0; i < items.size(); i++) {
      PushItem item = items.get(i);
      if (item.getKind() == PushItem.Kind.KIND_MESSAGE && PushDelivery.reliable(item.getFrame().getClass_())) {
        stream.ack(item.getDeliveryId(), com.pangate.contracts.v1.AckItem.Status.STATUS_NACK_CLOSED);
      } else if (item.getKind() == PushItem.Kind.KIND_BROADCAST) {
        frames.get(i).releaseShare();
      }
    }
  }

  void add(PushItem item, PreEncodedFrame frame) {
    items.add(item);
    frames.add(frame);
  }

  @Override
  public void run() {
    ReferenceOpenHashSet<Channel> written = new ReferenceOpenHashSet<>();
    for (int i = 0; i < items.size(); i++) {
      PushItem item = items.get(i);
      try {
        switch (item.getKind()) {
          case KIND_MESSAGE -> {
            Channel ch = handler.message(loop, item, stream);
            if (ch != null) {
              written.add(ch);
            }
          }
          case KIND_BROADCAST -> handler.broadcast(loop, item.getTopic(), item.getPriority(), frames.get(i));
          case KIND_CONTROL -> handler.control(loop, item.getPrincipalId(), item.getControl());
          default -> { }
        }
      } catch (RuntimeException e) {
        // One bad item must not cost the rest of the batch its delivery, acks or flush.
        errors.increment();
        log.warn("push item {} failed: {}", item.getKind(), e.toString());
      }
    }
    written.forEach(Channel::flush);
  }
}
