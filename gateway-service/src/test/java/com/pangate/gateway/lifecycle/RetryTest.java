package com.pangate.gateway.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.gateway.diagnostics.DependencyUnavailableException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class RetryTest {

  private final AtomicLong now = new AtomicLong();
  private final List<Duration> sleeps = new ArrayList<>();
  private final Retry.Sleeper sleeper = d -> {
    sleeps.add(d);
    now.addAndGet(d.toMillis());
  };

  @Test
  void retriesWithBackoffThenSucceeds() {
    AtomicInteger calls = new AtomicInteger();

    String r = Retry.untilDeadline("Kafka", "localhost:9092", "GATEWAY_KAFKA_BOOTSTRAP", Duration.ofSeconds(30),
        now::get, sleeper, () -> {
          if (calls.incrementAndGet() <= 3) {
            throw new IOException("down");
          }
          return "ok";
        });

    assertThat(r).isEqualTo("ok");
    assertThat(sleeps).containsExactly(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(4));
  }

  @Test
  void backoffCapsAtFiveSeconds() {
    AtomicInteger calls = new AtomicInteger();

    Retry.untilDeadline("Redis", "redis://x", "E", Duration.ofSeconds(60), now::get, sleeper, () -> {
      if (calls.incrementAndGet() <= 5) {
        throw new IOException("down");
      }
      return 1;
    });

    assertThat(sleeps).containsExactly(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(4),
        Duration.ofSeconds(5), Duration.ofSeconds(5));
  }

  @Test
  void throwsDependencyUnavailableAtDeadline() {
    IOException last = new IOException("refused");

    assertThatThrownBy(() -> Retry.untilDeadline("Kafka", "localhost:9092", "GATEWAY_KAFKA_BOOTSTRAP",
        Duration.ofSeconds(6), now::get, sleeper, () -> {
          throw last;
        }))
        .isInstanceOfSatisfying(DependencyUnavailableException.class, e -> {
          assertThat(e.target()).isEqualTo("localhost:9092");
          assertThat(e.envVar()).isEqualTo("GATEWAY_KAFKA_BOOTSTRAP");
          assertThat(e.getMessage()).isEqualTo("Kafka localhost:9092 is not reachable after 6 s");
          assertThat(e.getCause()).isSameAs(last);
        });
    assertThat(sleeps.stream().mapToLong(Duration::toMillis).sum()).isEqualTo(6_000);
    assertThat(sleeps).containsExactly(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(3));
  }
}
