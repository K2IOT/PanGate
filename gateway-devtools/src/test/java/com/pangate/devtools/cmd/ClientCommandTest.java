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
}
