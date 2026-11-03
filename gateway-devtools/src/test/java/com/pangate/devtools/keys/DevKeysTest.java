package com.pangate.devtools.keys;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DevKeysTest {

  @Test
  void createsKeyOnceAndReloadsSameKid(@TempDir Path d) throws Exception {
    ECKey a = DevKeys.loadOrCreateEs256(d.resolve("es256.pem"));
    ECKey b = DevKeys.loadOrCreateEs256(d.resolve("es256.pem"));

    assertThat(b.getKeyID()).isEqualTo(a.getKeyID()).isEqualTo(a.computeThumbprint().toString());
    assertThat(b.isPrivate()).isTrue();
    assertThat(a.getCurve()).isEqualTo(Curve.P_256);
    assertThat(Files.readString(d.resolve("es256.pem"))).startsWith("-----BEGIN EC PRIVATE KEY-----");
  }

  @Test
  void loadReadsExistingPem(@TempDir Path d) throws Exception {
    ECKey created = DevKeys.loadOrCreateEs256(d.resolve("k.pem"));

    assertThat(DevKeys.load(d.resolve("k.pem")).getKeyID()).isEqualTo(created.getKeyID());
  }

  @Test
  void writesPublicOnlyJwks(@TempDir Path d) throws Exception {
    ECKey key = DevKeys.loadOrCreateEs256(d.resolve("k.pem"));

    DevKeys.writeJwks(key, d.resolve("jwks.json"));

    JWKSet set = JWKSet.load(d.resolve("jwks.json").toFile());
    assertThat(set.getKeys()).hasSize(1);
    assertThat(set.getKeys().get(0).getKeyID()).isEqualTo(key.getKeyID());
    assertThat(set.getKeys().get(0).isPrivate()).isFalse();
  }
}
