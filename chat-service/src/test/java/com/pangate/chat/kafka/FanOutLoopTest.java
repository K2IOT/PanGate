package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

class FanOutLoopTest {

  /** Final review I3: a poll of fan-out work must finish well inside max.poll.interval.ms. */
  @Test
  void consumerTakesSmallPollsWithRoomToFinishThem() {
    var cfg = FanOutLoop.consumerConfig("k:9092", "g");
    assertThat(cfg.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(100);
    assertThat((Integer) cfg.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG))
        .isGreaterThan((int) FanOutLoop.STEP_BUDGET.toMillis() + (int) FanOutLoop.GIVE_UP.toMillis());
    assertThat(cfg.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
  }
}
