package com.pangate.pushapi.core;

import com.pangate.contracts.v1.PushRecord;

/** Produces to {@code cN.push.requests} and returns once the broker acknowledged it. */
@FunctionalInterface
public interface RecordProducer {
  /** @throws ProduceFailedException on timeout or a broker error */
  void send(PushRecord r);
}
