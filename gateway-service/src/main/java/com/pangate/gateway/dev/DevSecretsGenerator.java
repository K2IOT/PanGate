package com.pangate.gateway.dev;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyUse;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.io.pem.PemObject;

/**
 * Creates the dev secret layout (spec §9.1 / plan Global Constraints) and fills in only missing files, so tickets
 * and trusted CAs survive restarts. Never used outside profiles local and dev.
 */
public final class DevSecretsGenerator {

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final String[] TICKET_ROLES = {"prev", "current", "next"};

  private DevSecretsGenerator() {}

  public static void ensure(Path dir) {
    try {
      Files.createDirectories(dir);
      restrict(dir, "rwxr-xr-x");
      Files.createDirectories(dir.resolve("ticket-keys"));
      Files.createDirectories(dir.resolve("tls-ticket-keys"));
      ensureCertificates(dir);
      ensureEs256(dir);
      for (int i = 0; i < TICKET_ROLES.length; i++) {
        Path f = dir.resolve("ticket-keys").resolve(TICKET_ROLES[i]);
        if (!Files.exists(f)) {
          write(f, (i + 1) + " " + Base64.getEncoder().encodeToString(randomBytes(32)) + "\n");
        }
      }
      for (String role : TICKET_ROLES) {  // TLS session ticket keys (spec §7.7): 48 random bytes, base64
        Path f = dir.resolve("tls-ticket-keys").resolve(role);
        if (!Files.exists(f)) {
          write(f, Base64.getEncoder().encodeToString(randomBytes(48)) + "\n");
        }
      }
      Path hashKey = dir.resolve("principal-hash.key");
      if (!Files.exists(hashKey)) {
        write(hashKey, Base64.getEncoder().encodeToString(randomBytes(32)) + "\n");
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot create dev secrets in " + dir, e);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot create dev secrets in " + dir, e);
    }
  }

  private static void ensureCertificates(Path dir) throws Exception {
    Path caCrt = dir.resolve("ca.crt");
    Path caKey = dir.resolve("ca.key");
    if (!Files.exists(caCrt) || !Files.exists(caKey)) {
      KeyPair ca = ecKeyPair();
      X500Name name = new X500Name("CN=PanGate Dev CA");
      Instant now = Instant.now();
      JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(name, serial(), Date.from(now.minusSeconds(60)),
          Date.from(now.plus(Duration.ofDays(3650))), name, ca.getPublic());
      b.addExtension(Extension.basicConstraints, true, new BasicConstraints(0));
      b.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
      writePublic(caCrt, pem(certificate(b, ca.getPrivate())));
      write(caKey, pkcs8(ca.getPrivate()));
      Files.deleteIfExists(dir.resolve("gateway.crt"));
    }
    Path gwCrt = dir.resolve("gateway.crt");
    Path gwKey = dir.resolve("gateway.key");
    if (!Files.exists(gwCrt) || !Files.exists(gwKey)) {
      writeGatewayCert(dir);
    }
  }

  /**
   * Dev and tests only: signs a new gateway certificate (new key, new serial) with the existing dev CA. Each file is
   * replaced atomically, so a running gateway's reload sees either the old or the new file, never a partial one.
   */
  public static void reissueGatewayCert(Path dir) {
    try {
      writeGatewayCert(dir);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot reissue the dev gateway certificate in " + dir, e);
    }
  }

  private static void writeGatewayCert(Path dir) throws Exception {
    Path caCrt = dir.resolve("ca.crt");
    Path caKey = dir.resolve("ca.key");
    Path gwCrt = dir.resolve("gateway.crt");
    Path gwKey = dir.resolve("gateway.key");
    X509Certificate caCert = readCertificate(caCrt);
    PrivateKey caPrivate = readPrivateKey(caKey);
    KeyPair gw = ecKeyPair();
    Instant now = Instant.now();
    JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(caCert, serial(),
        Date.from(now.minusSeconds(60)), Date.from(now.plus(Duration.ofDays(825))),
        new X500Name("CN=localhost"), gw.getPublic());
    b.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(new GeneralName[] {
        new GeneralName(GeneralName.dNSName, "localhost"),
        new GeneralName(GeneralName.dNSName, "gateway"),
        new GeneralName(GeneralName.iPAddress, "127.0.0.1"),
        new GeneralName(GeneralName.iPAddress, InetAddress.getByName("::1").getHostAddress())}));
    b.addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(KeyPurposeId.id_kp_serverAuth));
    b.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature));
    writePublic(gwCrt, pem(certificate(b, caPrivate)));
    write(gwKey, pkcs8(gw.getPrivate()));
  }

  private static void ensureEs256(Path dir) throws Exception {
    Path pem = dir.resolve("es256.pem");
    if (!Files.exists(pem)) {
      KeyPair pair = ecKeyPair();
      SubjectPublicKeyInfo spki = SubjectPublicKeyInfo.getInstance(pair.getPublic().getEncoded());
      org.bouncycastle.asn1.sec.ECPrivateKey sec1 = new org.bouncycastle.asn1.sec.ECPrivateKey(
          256, ((ECPrivateKey) pair.getPrivate()).getS(), spki.getPublicKeyData(), spki.getAlgorithm().getParameters());
      write(pem, pem(new PemObject("EC PRIVATE KEY", sec1.getEncoded())));
      Files.deleteIfExists(dir.resolve("jwks.json"));
    }
    Path jwks = dir.resolve("jwks.json");
    if (!Files.exists(jwks)) {
      ECKey key = JWK.parseFromPEMEncodedObjects(Files.readString(pem, StandardCharsets.US_ASCII)).toECKey();
      ECKey pub = new ECKey.Builder(key.toPublicJWK()).keyID(key.computeThumbprint().toString())
          .keyUse(KeyUse.SIGNATURE).algorithm(JWSAlgorithm.ES256).build();
      writePublic(jwks, new JWKSet(pub).toString(true));
    }
  }

  private static KeyPair ecKeyPair() throws Exception {
    KeyPairGenerator gen = KeyPairGenerator.getInstance("EC");
    gen.initialize(new ECGenParameterSpec("secp256r1"), RANDOM);
    return gen.generateKeyPair();
  }

  private static X509Certificate certificate(JcaX509v3CertificateBuilder b, PrivateKey signer) throws Exception {
    ContentSigner cs = new JcaContentSignerBuilder("SHA256withECDSA").build(signer);
    return new JcaX509CertificateConverter().getCertificate(b.build(cs));
  }

  private static X509Certificate readCertificate(Path file) throws Exception {
    try (PEMParser p = new PEMParser(new StringReader(Files.readString(file)))) {
      return new JcaX509CertificateConverter().getCertificate((X509CertificateHolder) p.readObject());
    }
  }

  private static PrivateKey readPrivateKey(Path file) throws Exception {
    try (PEMParser p = new PEMParser(new StringReader(Files.readString(file)))) {
      return new JcaPEMKeyConverter().getPrivateKey((PrivateKeyInfo) p.readObject());
    }
  }

  private static String pkcs8(PrivateKey key) throws IOException {
    return pem(new JcaPKCS8Generator(key, null).generate());
  }

  private static String pem(Object obj) throws IOException {
    StringWriter out = new StringWriter();
    try (JcaPEMWriter w = new JcaPEMWriter(out)) {
      w.writeObject(obj);
    }
    return out.toString();
  }

  private static BigInteger serial() {
    return new BigInteger(64, RANDOM);
  }

  private static byte[] randomBytes(int n) {
    byte[] b = new byte[n];
    RANDOM.nextBytes(b);
    return b;
  }

  /** Private key material: owner read/write only. */
  private static void write(Path target, String content) throws IOException {
    write(target, content, "rw-------");
  }

  /** Certificates and the public JWKS: readable by other users (e.g. a non-root client container). */
  private static void writePublic(Path target, String content) throws IOException {
    write(target, content, "rw-r--r--");
  }

  private static void write(Path target, String content, String perms) throws IOException {
    Path tmp = Files.createTempFile(target.getParent(), ".tmp-", ".part");
    Files.writeString(tmp, content, StandardCharsets.US_ASCII);
    restrict(tmp, perms);
    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
  }

  private static void restrict(Path p, String perms) throws IOException {
    if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
      Files.setPosixFilePermissions(p, PosixFilePermissions.fromString(perms));
    }
  }
}
