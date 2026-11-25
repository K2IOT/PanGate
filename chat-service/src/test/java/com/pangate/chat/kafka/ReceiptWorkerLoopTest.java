package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.config.ChatProperties;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ReceiptWorkerLoopTest {

  static ChatProperties.PushApi pushApi(Duration timeout, Duration lowWait) {
    return new ChatProperties.PushApi("x", true, timeout, 4, 1_000, 0.2, lowWait, 10_000, Duration.ofSeconds(10),
        Duration.ofMinutes(5));
  }

  /** 3b minor: a push timeout plus low-wait past the receipt round deadline is found at start (resends otherwise). */
  @Test
  void roundDeadlineMustCoverTimeoutAndLowWait() {
    assertThat(ReceiptWorkerLoop.roundCovers(pushApi(Duration.ofSeconds(8), Duration.ofSeconds(2)))).isTrue();
    assertThat(ReceiptWorkerLoop.roundCovers(pushApi(Duration.ofSeconds(10), Duration.ofSeconds(2)))).isFalse();
  }
}
