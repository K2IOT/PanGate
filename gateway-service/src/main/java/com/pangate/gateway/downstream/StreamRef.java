package com.pangate.gateway.downstream;

import com.pangate.contracts.v1.AckItem;

/**
 * One router stream as seen by pending entries (spec §5.4, §6.3). Once the stream is gone its acks are dropped: the
 * new router replays from its watermark.
 */
public final class StreamRef {

  private final java.util.function.Consumer<AckItem> acks;
  private volatile boolean alive = true;

  public StreamRef(AckBatcher acks) {
    this(acks::offer);
  }

  /** Acks go to {@code sink} (any thread) while the stream is alive. */
  public StreamRef(java.util.function.Consumer<AckItem> sink) {
    this.acks = sink;
  }

  public boolean alive() {
    return alive;
  }

  public void markDead() {
    alive = false;
  }

  /** Any thread; a no-op once the stream is dead. */
  public void ack(long deliveryId, AckItem.Status status) {
    if (alive) {
      acks.accept(AckItem.newBuilder().setDeliveryId(deliveryId).setStatus(status).build());
    }
  }
}
