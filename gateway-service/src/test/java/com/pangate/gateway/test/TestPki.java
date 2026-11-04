package com.pangate.gateway.test;

import java.io.Writer;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/** A throwaway CA with server and SPIFFE client certificates (ECDSA P-256, PEM files) for mTLS tests. */
public final class TestPki {

  private static final AtomicLong SERIAL = new AtomicLong(System.currentTimeMillis());

  public record Issued(X509Certificate cert, PrivateKey key, Path certFile, Path keyFile) {}

  private final Path dir;
  private final KeyPair caKeys;
  private final X509Certificate ca;
  public final Path caFile;

  public TestPki(Path dir) throws Exception {
    this.dir = dir;
    caKeys = keys();
    X500Name name = new X500Name("CN=test-ca");
    JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(name, serial(), from(), to(), name,
        caKeys.getPublic());
    b.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
    ca = sign(b, caKeys.getPrivate());
    caFile = write("ca.crt", ca);
  }

  /** Server certificate for localhost / 127.0.0.1. */
  public Issued server() throws Exception {
    return issue("server", new GeneralName[] {new GeneralName(GeneralName.dNSName, "localhost"),
        new GeneralName(GeneralName.iPAddress, InetAddress.getLoopbackAddress().getHostAddress())});
  }

  /** Client certificate whose only SAN is the URI {@code spiffeId}. */
  public Issued client(String name, String spiffeId) throws Exception {
    return issue(name, new GeneralName[] {new GeneralName(GeneralName.uniformResourceIdentifier, spiffeId)});
  }

  private Issued issue(String name, GeneralName[] sans) throws Exception {
    KeyPair kp = keys();
    JcaX509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(ca, serial(), from(), to(),
        new X500Name("CN=" + name), kp.getPublic());
    b.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(sans));
    X509Certificate cert = sign(b, caKeys.getPrivate());
    return new Issued(cert, kp.getPrivate(), write(name + ".crt", cert), write(name + ".key", kp.getPrivate()));
  }

  private Path write(String file, Object pem) throws Exception {
    Path p = dir.resolve(file);
    try (Writer w = Files.newBufferedWriter(p); JcaPEMWriter pw = new JcaPEMWriter(w)) {
      pw.writeObject(pem instanceof PrivateKey k ? new org.bouncycastle.openssl.jcajce.JcaPKCS8Generator(k, null) : pem);
    }
    return p;
  }

  private static KeyPair keys() throws Exception {
    KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
    g.initialize(new ECGenParameterSpec("secp256r1"));
    return g.generateKeyPair();
  }

  private static X509Certificate sign(JcaX509v3CertificateBuilder b, PrivateKey key) throws Exception {
    return new JcaX509CertificateConverter().getCertificate(
        b.build(new JcaContentSignerBuilder("SHA256withECDSA").build(key)));
  }

  private static BigInteger serial() {
    return BigInteger.valueOf(SERIAL.incrementAndGet());
  }

  private static Date from() {
    return Date.from(Instant.now().minusSeconds(60));
  }

  private static Date to() {
    return Date.from(Instant.now().plusSeconds(3600));
  }
}
