package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class UserDirectoryTest {

  final FakeStores stores = new FakeStores();
  final AtomicLong nowMs = new AtomicLong(1_000_000);
  final UserDirectory users = new UserDirectory(stores, nowMs::get);

  /** Final review minor: a DM send must not read the users table every time. */
  @Test
  void knownStatesAreCached() {
    stores.upsertIfNewer("a", "active", 1);
    stores.upsertIfNewer("d", "deleted", 1);
    for (int i = 0; i < 5; i++) {
      assertThat(users.isDeleted("a")).isFalse();
      assertThat(users.isDeleted("d")).isTrue();
    }
    assertThat(stores.userReads.get()).isEqualTo(2);
  }

  /** An active user is re-read after 10 s (a deletion shows within 10 s); deleted is final and never re-read. */
  @Test
  void activeIsReReadAfterTenSecondsDeletedNever() {
    stores.upsertIfNewer("a", "active", 1);
    stores.upsertIfNewer("d", "deleted", 1);
    users.isDeleted("a");
    users.isDeleted("d");
    stores.upsertIfNewer("a", "deleted", 2);
    nowMs.addAndGet(10_001);
    assertThat(users.isDeleted("a")).isTrue();
    assertThat(users.isDeleted("d")).isTrue();
    assertThat(stores.userReads.get()).isEqualTo(3);
  }

  /** An unknown user is not cached: it may be registered a moment later. */
  @Test
  void unknownIsNotCached() {
    assertThat(users.isDeleted("x")).isFalse();
    stores.upsertIfNewer("x", "deleted", 1);
    assertThat(users.isDeleted("x")).isTrue();
  }
}
