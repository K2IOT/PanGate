package com.pangate.gateway.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Set;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class ConnIdTest {

  @Test
  void encodesTwentySixCrockfordChars() {
    String id = ConnId.next(1_762_000_000_000L, 3, (3L << 40) | 7, new SplittableRandom(1));

    assertThat(id).matches("^[0-9A-HJKMNP-TV-Z]{26}$");
    assertThat(ConnId.isValid(id)).isTrue();
  }

  @Test
  void roundTripsLoopAndSessionKey() {
    long key = (5L << 40) | 123_456L;

    String id = ConnId.next(1_762_000_000_000L, 5, key, new SplittableRandom(1));

    assertThat(ConnId.loopIdx(id)).isEqualTo(5);
    assertThat(ConnId.sessionKey(id)).isEqualTo(key);
  }

  @Test
  void roundTripsMaximumValues() {
    long key = (255L << 40) | 0xFF_FFFF_FFFFL;

    String id = ConnId.next((1L << 48) - 1, 255, key, new SplittableRandom(2));

    assertThat(ConnId.loopIdx(id)).isEqualTo(255);
    assertThat(ConnId.sessionKey(id)).isEqualTo(key);
  }

  @Test
  void sortsByTimestamp() {
    SplittableRandom r = new SplittableRandom(3);

    assertThat(ConnId.next(1000, 9, 1, r).compareTo(ConnId.next(2000, 0, 0, r))).isNegative();
  }

  @Test
  void uniqueAcross100kIds() {
    SplittableRandom r = new SplittableRandom(4);
    Set<String> ids = new HashSet<>();
    for (long i = 0; i < 100_000; i++) {
      ids.add(ConnId.next(1_762_000_000_000L, (int) (i % 16), ((i % 16) << 40) | i, r));
    }
    assertThat(ids).hasSize(100_000);
  }

  @Test
  void rejectsInvalid() {
    assertThat(ConnId.isValid("short")).isFalse();
    assertThat(ConnId.isValid("0123456789ABCDEFGHJKMNPQRI")).isFalse();
    assertThat(ConnId.isValid(null)).isFalse();
    assertThatThrownBy(() -> ConnId.loopIdx("I1234567890123456789012345")).isInstanceOf(IllegalArgumentException.class);
  }
}
