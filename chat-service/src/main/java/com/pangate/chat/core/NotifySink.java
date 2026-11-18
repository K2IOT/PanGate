package com.pangate.chat.core;

import com.pangate.contracts.v1.NotifyRequest;
import java.util.concurrent.CompletableFuture;

/** {@code cN.notify.requests} (CH4 spec §2.1): completes when Kafka acked, exceptionally when it did not. */
@FunctionalInterface
public interface NotifySink {
  CompletableFuture<Void> send(NotifyRequest r);
}
