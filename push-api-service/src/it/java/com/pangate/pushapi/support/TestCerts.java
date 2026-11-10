package com.pangate.pushapi.support;

import java.io.Writer;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Date;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/** A throwaway CA and leaf certificates with SPIFFE URI SANs, written as PEM files. */
public final class TestCerts {

  public record Leaf(Path cert, Path key) {}

  private final KeyPair caKeys;
  private final X509Certificate ca;
  private final Path dir;

  public TestCerts(Path dir) throws Exception {
    this.dir = dir;
    caKeys = keys();
    X500Name name = new X500Name("CN=it-ca");
    var b = new JcaX509v3CertificateBuilder(name, BigInteger.ONE, new Date(System.currentTimeMillis() - 60_000),
        new Date(System.currentTimeMillis() + 86_400_000), name, caKeys.getPublic());
    b.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
    ca = sign(b.build(new JcaContentSignerBuilder("SHA256withECDSA").build(caKeys.getPrivate())));
    write(dir.resolve("ca.pem"), ca);
  }

  public Path ca() {
    return dir.resolve("ca.pem");
  }

  private static KeyPair keys() throws Exception {
    KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
    g.initialize(256);
    return g.generateKeyPair();
  }

  private static X509Certificate sign(X509CertificateHolder h) throws Exception {
    return new JcaX509CertificateConverter().getCertificate(h);
  }

  /** A leaf for {@code spiffeId}, valid for localhost / 127.0.0.1 too (so it also serves as a server cert). */
  public Leaf leaf(String name, String spiffeId) throws Exception {
    KeyPair k = keys();
    var b = new JcaX509v3CertificateBuilder(new X500Name("CN=it-ca"), BigInteger.valueOf(System.nanoTime()),
        new Date(System.currentTimeMillis() - 60_000), new Date(System.currentTimeMillis() + 86_400_000),
        new X500Name("CN=" + name), k.getPublic());
    b.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(new GeneralName[] {
        new GeneralName(GeneralName.uniformResourceIdentifier, spiffeId),
        new GeneralName(GeneralName.dNSName, "localhost"), new GeneralName(GeneralName.iPAddress, "127.0.0.1")}));
    X509Certificate cert = sign(b.build(new JcaContentSignerBuilder("SHA256withECDSA").build(caKeys.getPrivate())));
    Leaf l = new Leaf(dir.resolve(name + ".crt"), dir.resolve(name + ".key"));
    write(l.cert(), cert);
    write(l.key(), k.getPrivate());
    return l;
  }

  private static void write(Path p, X509Certificate c) throws Exception {
    try (Writer w = Files.newBufferedWriter(p); JcaPEMWriter pem = new JcaPEMWriter(w)) {
      pem.writeObject(c);
    }
  }

  private static void write(Path p, PrivateKey k) throws Exception {
    try (Writer w = Files.newBufferedWriter(p); JcaPEMWriter pem = new JcaPEMWriter(w)) {
      pem.writeObject(new JcaPKCS8Generator(k, null));
    }
  }
}
