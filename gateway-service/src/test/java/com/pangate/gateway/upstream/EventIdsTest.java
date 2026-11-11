package com.pangate.gateway.upstream;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class EventIdsTest {

  static final String ULID = "[0-9A-HJKMNP-TV-Z]{26}";

  @Test
  void ulidShapeTimeOrderAndUniqueness() {
    SplittableRandom r = new SplittableRandom(1);
    String a = EventIds.next(1_000, r);
    String b = EventIds.next(2_000, r);
    assertThat(a).matches(ULID);
    assertThat(a.compareTo(b)).isNegative();
    assertThat(EventIds.next(1_762_732_800_000L, r)).startsWith("01K9NGZY00");  // 48-bit ms in Crockford base32
    Set<String> ids = new HashSet<>();
    for (int i = 0; i < 10_000; i++) {
      assertThat(ids.add(EventIds.next())).isTrue();
    }
  }
}
