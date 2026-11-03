package com.pangate.devtools.keys;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.JWSAlgorithm;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.util.io.pem.PemObject;

/**
 * Dev ES256 signing keys. Format contract shared with the gateway's dev secret generator:
 * SEC1 PEM ("BEGIN EC PRIVATE KEY") on disk, kid = RFC 7638 JWK thumbprint.
 */
public final class DevKeys {

  private DevKeys() {}

  /** Loads the key at {@code pem}, creating a new P-256 key there first if the file does not exist. */
  public static ECKey loadOrCreateEs256(Path pem) {
    if (Files.exists(pem)) {
      return load(pem);
    }
    try {
      KeyPairGenerator gen = KeyPairGenerator.getInstance("EC");
      gen.initialize(new ECGenParameterSpec("secp256r1"));
      KeyPair pair = gen.generateKeyPair();
      writeAtomically(pem, toSec1Pem(pair));
      return withThumbprintKid(new ECKey.Builder(Curve.P_256, (ECPublicKey) pair.getPublic())
          .privateKey((ECPrivateKey) pair.getPrivate()).build());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot create ES256 key at " + pem, e);
    }
  }

  /** Reads a SEC1 PEM private key; the kid is its JWK thumbprint. */
  public static ECKey load(Path pem) {
    try {
      JWK jwk = JWK.parseFromPEMEncodedObjects(Files.readString(pem, StandardCharsets.US_ASCII));
      return withThumbprintKid(jwk.toECKey());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (JOSEException e) {
      throw new IllegalStateException("Cannot parse ES256 key at " + pem, e);
    }
  }

  /** Writes a JWKS containing only the public part of {@code key}. */
  public static void writeJwks(ECKey key, Path file) {
    try {
      writeAtomically(file, new JWKSet(key.toPublicJWK()).toString(true));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** SEC1 "EC PRIVATE KEY" PEM that embeds the public key, so the key pair can be rebuilt from the file alone. */
  static String toSec1Pem(KeyPair pair) throws IOException {
    SubjectPublicKeyInfo spki = SubjectPublicKeyInfo.getInstance(pair.getPublic().getEncoded());
    org.bouncycastle.asn1.sec.ECPrivateKey sec1 = new org.bouncycastle.asn1.sec.ECPrivateKey(
        256, ((ECPrivateKey) pair.getPrivate()).getS(), spki.getPublicKeyData(), spki.getAlgorithm().getParameters());
    StringWriter out = new StringWriter();
    try (JcaPEMWriter writer = new JcaPEMWriter(out)) {
      writer.writeObject(new PemObject("EC PRIVATE KEY", sec1.getEncoded()));
    }
    return out.toString();
  }

  private static ECKey withThumbprintKid(ECKey key) throws JOSEException {
    return new ECKey.Builder(key).keyID(key.computeThumbprint().toString())
        .keyUse(KeyUse.SIGNATURE).algorithm(JWSAlgorithm.ES256).build();
  }

  private static void writeAtomically(Path target, String content) throws IOException {
    Path dir = target.toAbsolutePath().getParent();
    Files.createDirectories(dir);
    Path tmp = Files.createTempFile(dir, ".tmp-", ".part");
    Files.writeString(tmp, content, StandardCharsets.US_ASCII);
    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
  }
}
