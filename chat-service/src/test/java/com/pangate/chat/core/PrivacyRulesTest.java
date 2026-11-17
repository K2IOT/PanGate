package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Privacy;
import org.junit.jupiter.api.Test;

/** CH3 spec §8: hiding is reciprocal. */
class PrivacyRulesTest {

  static final Privacy OPEN = Privacy.OPEN;
  static final Privacy NO_PRESENCE = new Privacy(true, false);
  static final Privacy NO_READ = new Privacy(false, true);

  @Test
  void presenceIsReciprocal() {
    assertThat(PrivacyRules.mayShowPresence(OPEN, OPEN)).isTrue();
    assertThat(PrivacyRules.mayShowPresence(NO_PRESENCE, OPEN)).isFalse();
    assertThat(PrivacyRules.mayShowPresence(OPEN, NO_PRESENCE)).isFalse();
    assertThat(PrivacyRules.mayShowPresence(NO_READ, NO_READ)).isTrue();
  }

  @Test
  void readIsReciprocal() {
    assertThat(PrivacyRules.mayShowRead(OPEN, OPEN)).isTrue();
    assertThat(PrivacyRules.mayShowRead(NO_READ, OPEN)).isFalse();
    assertThat(PrivacyRules.mayShowRead(OPEN, NO_READ)).isFalse();
    assertThat(PrivacyRules.mayShowRead(NO_PRESENCE, NO_PRESENCE)).isTrue();
  }

  @Test
  void cacheKeeps60sAndForgets() {
    FakeStores stores = new FakeStores();
    java.util.concurrent.atomic.AtomicLong ms = new java.util.concurrent.atomic.AtomicLong();
    PrivacyCache cache = new PrivacyCache(stores, ms::get);
    assertThat(cache.of("u")).isEqualTo(OPEN);
    stores.putPrivacy("u", NO_READ, 0);
    assertThat(cache.of("u")).isEqualTo(OPEN);
    ms.set(60_001);
    assertThat(cache.of("u")).isEqualTo(NO_READ);
    stores.putPrivacy("u", OPEN, 0);
    cache.forget("u");
    assertThat(cache.of("u")).isEqualTo(OPEN);
  }
}
