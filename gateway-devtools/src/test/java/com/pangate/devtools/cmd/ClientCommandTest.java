package com.pangate.devtools.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ClientCommandTest {

  @Test
  void loadPingsAreSpreadOverTheIntervalNotSentAsOneBurst() {
    Duration interval = Duration.ofSeconds(30);

    assertThat(ClientCommand.pingOffsetNanos(0, 4, interval)).isZero();
    assertThat(ClientCommand.pingOffsetNanos(1, 4, interval)).isEqualTo(Duration.ofMillis(7_500).toNanos());
    assertThat(ClientCommand.pingOffsetNanos(3, 4, interval)).isEqualTo(Duration.ofMillis(22_500).toNanos());
    // 18k sessions: consecutive pings ~1.7 ms apart, the last one still inside the interval
    assertThat(ClientCommand.pingOffsetNanos(17_999, 18_000, interval)).isLessThan(interval.toNanos());
    assertThat(ClientCommand.pingOffsetNanos(1, 18_000, interval)).isEqualTo(interval.toNanos() / 18_000);
  }

  /** CI load check: a run that misses its session count must say why the sessions failed. */
  @Test
  void loadFailuresAreCountedByReason() {
    ClientCommand.Failures f = new ClientCommand.Failures();
    f.record(new IllegalStateException("HELLO timed out"));
    f.record(new IllegalStateException("HELLO timed out"));
    f.record(new java.io.UncheckedIOException(new java.io.IOException("Connection reset")));
    assertThat(f.total()).isEqualTo(3);
    assertThat(f.summary())
        .isEqualTo("reasons={IllegalStateException: HELLO timed out=2, UncheckedIOException: Connection reset=1}");
    assertThat(new ClientCommand.Failures().summary()).isEmpty();
  }

  /**
   * CI load check: a 503 at the upgrade is the gateway shedding (L3, spec §7.3, with Retry-After); a real client tries
   * again later, so the load client does too. Other failures stay failures.
   */
  @Test
  void onlyASheddingRefusalIsRetried() {
    assertThat(ClientCommand.shedRefusal(
        new IllegalStateException("Invalid handshake response getStatus: 503 Service Unavailable"))).isTrue();
    assertThat(ClientCommand.shedRefusal(new IllegalStateException("HELLO timed out"))).isFalse();
    assertThat(ClientCommand.shedRefusal(
        new IllegalStateException("Invalid handshake response getStatus: 401 Unauthorized"))).isFalse();
    assertThat(ClientCommand.shedRefusal(new RuntimeException(
        new IllegalStateException("Invalid handshake response getStatus: 503 Service Unavailable")))).isTrue();
  }

  /** Spread like the gateway's own Retry-After (5..30 s), so retries do not come back as one burst. */
  @Test
  void retriesWaitFiveToFifteenSeconds() {
    java.util.Random rnd = new java.util.Random(7);
    for (int i = 0; i < 1_000; i++) {
      assertThat(ClientCommand.retryDelayMs(rnd)).isBetween(5_000L, 15_000L);
    }
  }
}
