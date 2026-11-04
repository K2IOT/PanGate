package com.pangate.gateway.server;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Single "gw-tick" thread for periodic node housekeeping (samplers, JWKS refresh). Never runs I/O itself. */
public final class TickScheduler {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TickScheduler.class);

  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "gw-tick");
    t.setDaemon(true);
    return t;
  });

  public ScheduledExecutorService executor() {
    return executor;
  }

  /**
   * A task that throws keeps its schedule (a plain {@code scheduleAtFixedRate} would silently cancel it, freezing e.g.
   * the shed level); the failure is logged at WARN at most once a minute per task.
   */
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Duration period) {
    long[] lastLogMs = {Long.MIN_VALUE / 2};
    Runnable guarded = () -> {
      try {
        task.run();
      } catch (Throwable t) {
        long now = System.currentTimeMillis();
        if (now - lastLogMs[0] >= 60_000) {
          lastLogMs[0] = now;
          log.warn("gw-tick task {} failed; it keeps its schedule", task, t);
        }
      }
    };
    return executor.scheduleAtFixedRate(guarded, period.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
  }

  public void shutdown() {
    executor.shutdownNow();
  }
}
