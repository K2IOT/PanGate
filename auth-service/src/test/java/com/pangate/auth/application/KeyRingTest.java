package com.pangate.auth.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.jwk.JWKSet;
import com.pangate.auth.application.port.SigningKeyStore.PublishedKey;
import com.pangate.auth.application.port.SigningKeyStore.Snapshot;
import com.pangate.auth.domain.KeyState;
import com.pangate.auth.infrastructure.crypto.Es256TokenSigner;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class KeyRingTest {

  @Test
  void jwksAndVerificationKeysIncludeKeysThisPodCannotDecrypt() throws Exception {
    var mine = Es256TokenSigner.newKey(Instant.now(), KeyState.ACTIVE);
    var other = Es256TokenSigner.newKey(Instant.now(), KeyState.RETIRED);
    var store = new InMemorySigningKeyStore() {
      @Override
      public Snapshot load() {
        return new Snapshot(List.of(mine), List.of(
            new PublishedKey(mine.kid(), KeyState.ACTIVE, mine.privateJwk().toPublicJWK(), "v1", true),
            new PublishedKey(other.kid(), KeyState.RETIRED, other.privateJwk().toPublicJWK(), "v2", false)));
      }
    };
    KeyRing ring = new KeyRing(store);
    ring.reload();

    assertThat(JWKSet.parse(ring.jwksJson()).getKeys()).extracting(k -> k.getKeyID())
        .containsExactlyInAnyOrder(mine.kid(), other.kid());
    assertThat(JWKSet.parse(ring.jwksJson()).getKeys()).allSatisfy(k -> assertThat(k.isPrivate()).isFalse());
    assertThat(ring.publicKey(other.kid())).isPresent();
    assertThat(ring.active().kid()).isEqualTo(mine.kid());
    assertThat(ring.count(KeyState.RETIRED)).isEqualTo(1);
  }
}
