package com.pangate.auth.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.auth.config.AuthProperties;
import com.pangate.auth.domain.KeyState;
import com.pangate.auth.domain.SigningKey;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RotateSigningKeysTest {

  static final Instant T0 = Instant.parse("2026-11-01T00:00:00Z");
  static final AuthProperties.Keys KEYS = new AuthProperties.Keys(Duration.ofDays(30), Duration.ofDays(7),
      Duration.ofHours(1), "v1", Duration.ofSeconds(60), Duration.ofHours(1));

  private final AtomicReference<Instant> now = new AtomicReference<>(T0);
  private final InMemorySigningKeyStore store = new InMemorySigningKeyStore();
  private final RotateSigningKeys rotate = new RotateSigningKeys(store, now::get, KEYS);

  private void at(Duration sinceT0) {
    now.set(T0.plus(sinceT0));
  }

  @Test
  void firstRunCreatesAndActivatesAKeyAtOnce() {
    assertThat(rotate.runOnce()).isTrue();
    assertThat(store.rows).hasSize(1);
    SigningKey k = store.in(KeyState.ACTIVE).getFirst();
    assertThat(k.activateAt()).isEqualTo(T0);
  }

  @Test
  void noChangeWhenNothingIsDue() {
    rotate.runOnce();
    at(Duration.ofDays(28));
    assertThat(rotate.runOnce()).isFalse();
  }

  @Test
  void pendingKeyAppearsOneDayBeforeExpiry() {
    rotate.runOnce();
    at(Duration.ofDays(29));
    assertThat(rotate.runOnce()).isTrue();
    assertThat(store.in(KeyState.PENDING)).singleElement()
        .extracting(SigningKey::activateAt).isEqualTo(now.get().plus(Duration.ofHours(1)));
    assertThat(rotate.runOnce()).isFalse();  // only one pending
  }

  @Test
  void pendingIsActivatedAfterPublishAhead() {
    rotate.runOnce();
    String first = store.in(KeyState.ACTIVE).getFirst().kid();
    at(Duration.ofDays(29));
    rotate.runOnce();
    String second = store.in(KeyState.PENDING).getFirst().kid();
    at(Duration.ofDays(29).plusHours(1));

    assertThat(rotate.runOnce()).isTrue();

    assertThat(store.in(KeyState.ACTIVE)).singleElement().extracting(SigningKey::kid).isEqualTo(second);
    SigningKey old = store.rows.get(first);
    assertThat(old.state()).isEqualTo(KeyState.RETIRED);
    assertThat(old.retireAt()).isEqualTo(now.get());
    assertThat(old.removeAt()).isEqualTo(now.get().plus(Duration.ofDays(7)));
  }

  @Test
  void retiredKeyIsDeletedAfterOverlap() {
    rotate.runOnce();
    String first = store.in(KeyState.ACTIVE).getFirst().kid();
    at(Duration.ofDays(29));
    rotate.runOnce();
    at(Duration.ofDays(29).plusHours(1));
    rotate.runOnce();
    at(Duration.ofDays(36).plusHours(1).minusSeconds(1));
    rotate.runOnce();
    assertThat(store.rows).containsKey(first);
    at(Duration.ofDays(36).plusHours(1));
    assertThat(rotate.runOnce()).isTrue();
    assertThat(store.rows).doesNotContainKey(first).hasSize(1);
  }

  /** A view of {@link #store} in which this pod cannot decrypt {@code hidden} (other pods may). */
  private InMemorySigningKeyStore viewWithout(SigningKey hidden, boolean otherRowsWithSameKekDecrypt) {
    return new InMemorySigningKeyStore() {
      @Override
      public Snapshot load() {
        var usable = store.all().stream().filter(k -> !k.kid().equals(hidden.kid())).toList();
        var published = store.all().stream().map(k -> new PublishedKey(k.kid(), k.state(),
            k.privateJwk().toPublicJWK(), "v1", !k.kid().equals(hidden.kid()))).toList();
        return new Snapshot(otherRowsWithSameKekDecrypt ? usable : java.util.List.of(), published.stream()
            .map(p -> p.kid().equals(hidden.kid()) || otherRowsWithSameKekDecrypt ? p
                : new PublishedKey(p.kid(), p.state(), p.publicJwk(), p.kekId(), false)).toList());
      }

      @Override
      public java.util.List<SigningKey> all() {
        return load().usable();
      }

      @Override
      public void insert(SigningKey k) {
        store.insert(k);
      }

      @Override
      public void update(SigningKey k) {
        store.update(k);
      }

      @Override
      public int retireActive(Instant retireAt, Instant removeAt) {
        return store.retireActive(retireAt, removeAt);
      }
    };
  }

  private SigningKey rotatedTwice() {
    rotate.runOnce();
    at(Duration.ofDays(29));
    rotate.runOnce();
    at(Duration.ofDays(29).plusHours(1));
    rotate.runOnce();  // first key retired, second active
    return store.in(KeyState.ACTIVE).getFirst();
  }

  @Test
  void corruptActiveKeyIsReplacedWhenTheSameKekOpensOtherKeys() {
    SigningKey active = rotatedTwice();
    assertThat(new RotateSigningKeys(viewWithout(active, true), now::get, KEYS).runOnce()).isTrue();
    assertThat(store.in(KeyState.ACTIVE)).singleElement().extracting(SigningKey::kid).isNotEqualTo(active.kid());
    assertThat(store.rows.get(active.kid()).state()).isEqualTo(KeyState.RETIRED);
  }

  @Test
  void activeKeyThisPodCannotReadIsLeftAloneWhenItsKekIsUnproven() {
    SigningKey active = rotatedTwice();
    assertThat(new RotateSigningKeys(viewWithout(active, false), now::get, KEYS).runOnce()).isFalse();
    assertThat(store.in(KeyState.ACTIVE)).singleElement().extracting(SigningKey::kid).isEqualTo(active.kid());
  }

  @Test
  void forcePendingPublishesANewKeyAhead() {
    rotate.runOnce();
    assertThat(rotate.forcePending()).isTrue();
    assertThat(store.in(KeyState.PENDING)).singleElement()
        .extracting(SigningKey::activateAt).isEqualTo(T0.plus(Duration.ofHours(1)));
    assertThat(rotate.forcePending()).isFalse();  // a pending key already exists
  }
}
