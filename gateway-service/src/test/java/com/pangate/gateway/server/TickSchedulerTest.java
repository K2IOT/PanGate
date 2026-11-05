package com.pangate.gateway.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

class TickSchedulerTest {

  @Test
  void throwingTaskKeepsRunning() {
    TickScheduler tick = new TickScheduler();
    AtomicInteger runs = new AtomicInteger();
    try {
      tick.scheduleAtFixedRate(() -> {
        runs.incrementAndGet();
        throw new IllegalStateException("boom");
      }, Duration.ofMillis(10));
      Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> runs.get() >= 3);
    } finally {
      tick.shutdown();
    }
  }
}
