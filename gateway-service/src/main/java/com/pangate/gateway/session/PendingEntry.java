package com.pangate.gateway.session;

import com.pangate.contracts.v1.AckItem;
import com.pangate.gateway.downstream.StreamRef;

/**
 * A RELIABLE / ORDERED push written to the client and waiting for its ACK (spec §5.3, §5.4). {@code previous} is an
 * earlier delivery of the same message on the same router stream: it is answered together with this one, so every
 * delivery id gets exactly one ack.
 */
public record PendingEntry(String msgId, long deliveryId, StreamRef stream, long sentAtSec, int bytes,
    PendingEntry previous) {

  public PendingEntry(String msgId, long deliveryId, StreamRef stream, long sentAtSec, int bytes) {
    this(msgId, deliveryId, stream, sentAtSec, bytes, null);
  }

  /** Acks this delivery and every earlier one it replaced on the same stream. */
  public void answer(AckItem.Status status) {
    for (PendingEntry e = this; e != null; e = e.previous) {
      e.stream.ack(e.deliveryId, status);
    }
  }
}
