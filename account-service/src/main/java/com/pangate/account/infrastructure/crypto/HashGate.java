package com.pangate.account.infrastructure.crypto;

import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

/**
 * Bounds concurrent password hashes per pod (account spec §7): each takes ~50 ms CPU and 19 MiB, so an unbounded
 * burst of logins would exhaust memory. A request that finds no free slot fails at once instead of queueing.
 */
public final class HashGate {

  /** Spec §7 metrics: {@code account.password.hash.latency}, {@code account.password.hash.rejected}. */
  public interface Observer {
    Observer NONE = new Observer() {
      @Override
      public void took(java.time.Duration d) {}

      @Override
      public void rejected() {}
    };

    void took(java.time.Duration d);

    void rejected();
  }

  private final Semaphore slots;
  private final Observer observer;

  public HashGate(int permits) {
    this(permits, Observer.NONE);
  }

  public HashGate(int permits, Observer observer) {
    this.slots = new Semaphore(permits);
    this.observer = observer;
  }

  /** {@code configured}, or 2 × available processors when it is 0. */
  public static int permits(int configured) {
    return configured > 0 ? configured : 2 * Runtime.getRuntime().availableProcessors();
  }

  public <T> T run(Supplier<T> work) {
    if (!slots.tryAcquire()) {
      observer.rejected();
      throw new BusyException();
    }
    long t0 = System.nanoTime();
    try {
      return work.get();
    } finally {
      slots.release();
      observer.took(java.time.Duration.ofNanos(System.nanoTime() - t0));
    }
  }
}
