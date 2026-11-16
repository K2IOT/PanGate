package com.pangate.account.infrastructure.crypto;

import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

/**
 * Bounds concurrent password hashes per pod (account spec §7): each takes ~50 ms CPU and 19 MiB, so an unbounded
 * burst of logins would exhaust memory. A request that finds no free slot fails at once instead of queueing.
 */
public final class HashGate {

  private final Semaphore slots;

  public HashGate(int permits) {
    this.slots = new Semaphore(permits);
  }

  /** {@code configured}, or 2 × available processors when it is 0. */
  public static int permits(int configured) {
    return configured > 0 ? configured : 2 * Runtime.getRuntime().availableProcessors();
  }

  public <T> T run(Supplier<T> work) {
    if (!slots.tryAcquire()) {
      throw new BusyException();
    }
    try {
      return work.get();
    } finally {
      slots.release();
    }
  }
}
