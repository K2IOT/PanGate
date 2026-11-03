package com.pangate.gateway.lifecycle;

import com.pangate.gateway.diagnostics.DependencyUnavailableException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.LongSupplier;

/** Bounded startup wait for a dependency (spec §3.7): backoff 1, 2, 4, 5, 5… s until the deadline. */
public final class Retry {

  private static final long MAX_BACKOFF_MS = 5_000;

  @FunctionalInterface
  public interface Sleeper {
    void sleep(Duration d) throws InterruptedException;

    Sleeper SYSTEM = d -> Thread.sleep(d.toMillis());
  }

  private Retry() {}

  public static <T> T untilDeadline(String dependency, String target, String envVar, Duration timeout,
      LongSupplier nowMillis, Sleeper sleeper, Callable<T> attempt) {
    long deadline = nowMillis.getAsLong() + timeout.toMillis();
    long backoff = 1_000;
    while (true) {
      Exception last;
      try {
        return attempt.call();
      } catch (Exception e) {
        last = e;
      }
      long remaining = deadline - nowMillis.getAsLong();
      if (remaining <= 0) {
        throw new DependencyUnavailableException(dependency, target, timeout, envVar, last);
      }
      try {
        sleeper.sleep(Duration.ofMillis(Math.min(backoff, remaining)));
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new DependencyUnavailableException(dependency, target, timeout, envVar, last);
      }
      backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
    }
  }
}
