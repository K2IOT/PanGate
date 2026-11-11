package com.pangate.pushapi.core;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Runs {@code work} so that each caller returns after a run that started after its call, while callers that arrive
 * together share one run. A {@link ReentrantLock} rather than {@code synchronized}: waiting virtual threads unmount
 * instead of pinning their carriers.
 */
public final class SingleFlight {

  private final ReentrantLock lock = new ReentrantLock();
  private long started;      // guarded by lock
  private volatile long startedSeen;
  private long finished;     // guarded by lock

  public void run(Runnable work) {
    long need = startedSeen + 1;   // a run numbered at least this starts after now
    lock.lock();
    try {
      if (finished >= need) {
        return;
      }
      long n = ++started;
      startedSeen = n;
      try {
        work.run();
      } finally {
        finished = n;
      }
    } finally {
      lock.unlock();
    }
  }
}
