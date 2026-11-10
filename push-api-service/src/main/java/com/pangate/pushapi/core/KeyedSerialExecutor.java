package com.pangate.pushapi.core;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Work of one key runs in submit order, work of different keys concurrently (push-api spec §2.2, §2.3): a batch or a
 * gRPC stream keeps each principal's pushes in order without serializing everything.
 */
public final class KeyedSerialExecutor {

  private final Executor executor;
  private final Map<String, CompletableFuture<Void>> tails = new ConcurrentHashMap<>();

  public KeyedSerialExecutor(Executor executor) {
    this.executor = executor;
  }

  public <T> CompletableFuture<T> submit(String key, Supplier<T> work) {
    CompletableFuture<T> result = new CompletableFuture<>();
    CompletableFuture<Void> mine = tails.compute(key, (k, prev) -> {
      CompletableFuture<Void> start = prev == null ? CompletableFuture.completedFuture(null) : prev;
      return start.thenRunAsync(() -> {
        try {
          result.complete(work.get());
        } catch (Throwable t) {
          result.completeExceptionally(t);
        }
      }, executor);
    });
    mine.whenComplete((v, e) -> tails.remove(key, mine));
    return result;
  }

  /** Keys with work queued or running (tests, metrics). */
  public int pendingKeys() {
    return tails.size();
  }
}
