package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/** CH4 spec §3.3: muted until a time, for ever, or not; a mention still notifies. */
class MutesTest {

  static final long NOW = 1_800_000_000_000L;

  @Test
  void mutedUntilAFutureTime() {
    assertThat(Mutes.muted(0, NOW)).isFalse();
    assertThat(Mutes.muted(NOW - 1, NOW)).isFalse();
    assertThat(Mutes.muted(NOW + 1, NOW)).isTrue();
    assertThat(Mutes.muted(Long.MAX_VALUE, NOW)).isTrue();
  }

  @Test
  void aMentionStillNotifies() {
    assertThat(Mutes.shouldNotify(Long.MAX_VALUE, List.of(), "u1", NOW)).isFalse();
    assertThat(Mutes.shouldNotify(Long.MAX_VALUE, List.of("u2", "u1"), "u1", NOW)).isTrue();
    assertThat(Mutes.shouldNotify(0, List.of(), "u1", NOW)).isTrue();
  }
}
