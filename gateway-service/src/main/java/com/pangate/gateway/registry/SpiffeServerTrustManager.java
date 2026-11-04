package com.pangate.gateway.registry;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Server check for SPIFFE peers (spec §7.7): the chain must verify against the trust bundle and the leaf must carry a
 * {@code spiffe://} URI SAN. There is no hostname check: SVIDs identify workloads by URI, not by DNS name, so the
 * JDK/Netty endpoint identification would reject every SVID.
 */
final class SpiffeServerTrustManager extends X509ExtendedTrustManager {

  private final X509TrustManager delegate;

  SpiffeServerTrustManager(X509TrustManager delegate) {
    this.delegate = delegate;
  }

  static SpiffeServerTrustManager fromBundle(Path pem) throws IOException, GeneralSecurityException {
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null, null);
    try (InputStream in = Files.newInputStream(pem)) {
      int i = 0;
      for (Certificate c : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
        ks.setCertificateEntry("ca-" + i++, c);
      }
    }
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ks);
    for (var tm : tmf.getTrustManagers()) {
      if (tm instanceof X509TrustManager x) {
        return new SpiffeServerTrustManager(x);
      }
    }
    throw new GeneralSecurityException("no X509TrustManager for " + pem);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    delegate.checkServerTrusted(chain, authType);  // chain only: this overload does no endpoint identification
    if (!hasSpiffeId(chain[0])) {
      throw new CertificateException("server certificate has no spiffe:// URI SAN");
    }
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
      throws CertificateException {
    checkServerTrusted(chain, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
      throws CertificateException {
    checkServerTrusted(chain, authType);
  }

  private static boolean hasSpiffeId(X509Certificate cert) throws CertificateException {
    Collection<List<?>> sans = cert.getSubjectAlternativeNames();
    if (sans != null) {
      for (List<?> san : sans) {
        if (san.size() == 2 && Integer.valueOf(6).equals(san.get(0))
            && String.valueOf(san.get(1)).startsWith("spiffe://")) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    throw new CertificateException("client-side trust manager only");
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
      throws CertificateException {
    checkClientTrusted(chain, authType);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
      throws CertificateException {
    checkClientTrusted(chain, authType);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return delegate.getAcceptedIssuers();
  }
}
