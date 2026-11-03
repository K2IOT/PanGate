package com.pangate.gateway.server;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Single "gw-tick" thread for periodic node housekeeping (samplers, JWKS refresh). Never runs I/O itself. */
public final class TickScheduler {

  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "gw-tick");
    t.setDaemon(true);
    return t;
  });

  public ScheduledExecutorService executor() {
    return executor;
  }

  public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Duration period) {
    return executor.scheduleAtFixedRate(task, period.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
  }

  public void shutdown() {
    executor.shutdownNow();
  }
}
