package com.pangate.gateway.session;

import com.pangate.gateway.downstream.StreamRef;

/** A RELIABLE / ORDERED push written to the client and waiting for its ACK (spec §5.3, §5.4). */
public record PendingEntry(String msgId, long deliveryId, StreamRef stream, long sentAtSec, int bytes) {}
