package com.pangate.notify.application.port;

import java.util.concurrent.CompletableFuture;

/** Completes after {@code ms}; the pause between two tries of one token. */
@FunctionalInterface
public interface Delayer {
  CompletableFuture<Void> after(long ms);
}
