package com.pangate.gateway.downstream;

import com.pangate.contracts.v1.AckItem;

/**
 * One router stream as seen by pending entries (spec §5.4, §6.3). Once the stream is gone its acks are dropped: the
 * new router replays from its watermark.
 */
public final class StreamRef {

  private final AckBatcher acks;
  private volatile boolean alive = true;

  public StreamRef(AckBatcher acks) {
    this.acks = acks;
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
      acks.offer(AckItem.newBuilder().setDeliveryId(deliveryId).setStatus(status).build());
    }
  }
}
