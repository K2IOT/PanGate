package com.pangate.gateway.dev;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPublicKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DevSecretsGeneratorTest {

  static final List<String> FILES = List.of("ca.crt", "ca.key", "gateway.crt", "gateway.key", "es256.pem",
      "jwks.json", "ticket-keys/prev", "ticket-keys/current", "ticket-keys/next", "principal-hash.key",
      "tls-ticket-keys/prev", "tls-ticket-keys/current", "tls-ticket-keys/next");

  @Test
  void generatesAllFilesWithExpectedFormats(@TempDir Path d) throws Exception {
    DevSecretsGenerator.ensure(d);

    for (String f : FILES) {
      assertThat(d.resolve(f)).exists();
    }
    assertThat(Files.readString(d.resolve("gateway.key"))).startsWith("-----BEGIN PRIVATE KEY-----");
    assertThat(Files.readString(d.resolve("es256.pem"))).startsWith("-----BEGIN EC PRIVATE KEY-----");

    X509Certificate ca = cert(d.resolve("ca.crt"));
    X509Certificate gw = cert(d.resolve("gateway.crt"));
    gw.verify(ca.getPublicKey());
    assertThat(gw.getPublicKey()).isInstanceOf(ECPublicKey.class);
    assertThat(((ECPublicKey) gw.getPublicKey()).getParams().getCurve().getField().getFieldSize()).isEqualTo(256);
    assertThat(gw.getSubjectAlternativeNames()).extracting(l -> l.get(1).toString())
        .contains("localhost", "gateway", "127.0.0.1");
    assertThat(gw.getExtendedKeyUsage()).contains("1.3.6.1.5.5.7.3.1");

    JWKSet jwks = JWKSet.load(d.resolve("jwks.json").toFile());
    JWK es256 = JWK.parseFromPEMEncodedObjects(Files.readString(d.resolve("es256.pem")));
    assertThat(jwks.getKeys()).singleElement().satisfies(k -> {
      assertThat(k.getKeyID()).isEqualTo(es256.computeThumbprint().toString());
      assertThat(k.isPrivate()).isFalse();
    });

    assertThat(Files.readString(d.resolve("ticket-keys/prev")).trim()).matches("^1 [A-Za-z0-9+/]{43}=$");
    assertThat(Files.readString(d.resolve("ticket-keys/current")).trim()).matches("^2 [A-Za-z0-9+/]{43}=$");
    assertThat(Files.readString(d.resolve("ticket-keys/next")).trim()).matches("^3 [A-Za-z0-9+/]{43}=$");
    for (String r : List.of("prev", "current", "next")) {
      assertThat(java.util.Base64.getDecoder().decode(Files.readString(d.resolve("tls-ticket-keys/" + r)).trim()))
          .hasSize(48);
    }
  }

  @Test
  void privateKeysOwnerOnlyPublicFilesReadable(@TempDir Path d) throws Exception {
    DevSecretsGenerator.ensure(d);

    for (String f : List.of("ca.key", "gateway.key", "es256.pem", "ticket-keys/current", "principal-hash.key")) {
      assertThat(java.nio.file.attribute.PosixFilePermissions.toString(Files.getPosixFilePermissions(d.resolve(f))))
          .as(f).isEqualTo("rw-------");
    }
    for (String f : List.of("ca.crt", "gateway.crt", "jwks.json")) {
      assertThat(java.nio.file.attribute.PosixFilePermissions.toString(Files.getPosixFilePermissions(d.resolve(f))))
          .as(f).isEqualTo("rw-r--r--");
    }
    assertThat(java.nio.file.attribute.PosixFilePermissions.toString(Files.getPosixFilePermissions(d)))
        .isEqualTo("rwxr-xr-x");
  }

  @Test
  void secondRunKeepsExistingFiles(@TempDir Path d) throws Exception {
    DevSecretsGenerator.ensure(d);
    Map<String, byte[]> before = snapshot(d);

    DevSecretsGenerator.ensure(d);

    assertThat(snapshot(d)).containsExactlyInAnyOrderEntriesOf(before);
  }

  @Test
  void fillsOnlyMissingFiles(@TempDir Path d) throws Exception {
    DevSecretsGenerator.ensure(d);
    String kid = JWKSet.load(d.resolve("jwks.json").toFile()).getKeys().get(0).getKeyID();
    byte[] cert = Files.readAllBytes(d.resolve("gateway.crt"));
    Files.delete(d.resolve("jwks.json"));

    DevSecretsGenerator.ensure(d);

    assertThat(JWKSet.load(d.resolve("jwks.json").toFile()).getKeys().get(0).getKeyID()).isEqualTo(kid);
    assertThat(Files.readAllBytes(d.resolve("gateway.crt"))).isEqualTo(cert);
  }

  private static X509Certificate cert(Path p) throws Exception {
    try (InputStream in = Files.newInputStream(p)) {
      return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(in);
    }
  }

  private static Map<String, byte[]> snapshot(Path d) throws Exception {
    Map<String, byte[]> m = new HashMap<>();
    try (Stream<Path> s = Files.walk(d)) {
      for (Path p : s.filter(Files::isRegularFile).toList()) {
        m.put(d.relativize(p).toString(), Files.readAllBytes(p));
      }
    }
    return m;
  }
}
