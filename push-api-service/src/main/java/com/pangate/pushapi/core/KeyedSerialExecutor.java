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
    Object[] out = new Object[1];
    Throwable[] err = new Throwable[1];
    CompletableFuture<Void> mine = tails.compute(key, (k, prev) -> {
      CompletableFuture<Void> start = prev == null ? CompletableFuture.completedFuture(null) : prev;
      return start.thenRunAsync(() -> {
        try {
          out[0] = work.get();
        } catch (Throwable t) {
          err[0] = t;
        }
      }, executor);
    });
    // The key is released before the result completes: whoever sees the result never sees its key still pending
    // (a flaky pendingKeys() == 1 on CI). mine's completion publishes out/err to this callback.
    mine.whenComplete((v, e) -> {
      tails.remove(key, mine);
      if (err[0] != null) {
        result.completeExceptionally(err[0]);
      } else {
        @SuppressWarnings("unchecked")
        T value = (T) out[0];
        result.complete(value);
      }
    });
    return result;
  }

  /** Keys with work queued or running (tests, metrics). */
  public int pendingKeys() {
    return tails.size();
  }
}
