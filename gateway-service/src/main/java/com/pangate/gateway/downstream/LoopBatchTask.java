package com.pangate.gateway.downstream;

import com.pangate.contracts.v1.PushItem;
import com.pangate.gateway.loop.LoopContext;
import io.netty.channel.Channel;
import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;
import java.util.ArrayList;
import java.util.List;

/** The items of one batch for one event loop: handled in order on that loop, then each written channel flushed once. */
final class LoopBatchTask implements Runnable {

  private final LoopContext loop;
  private final LoopItemHandler handler;
  private final StreamRef stream;
  private final List<PushItem> items = new ArrayList<>();
  private final List<PreEncodedFrame> frames = new ArrayList<>();

  LoopBatchTask(LoopContext loop, LoopItemHandler handler, StreamRef stream) {
    this.loop = loop;
    this.handler = handler;
    this.stream = stream;
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
    }
    written.forEach(Channel::flush);
  }
}
