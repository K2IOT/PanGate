package com.pangate.devtools.client;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.nio.file.Path;
import javax.net.ssl.SSLException;

/** Client TLS settings: trust a dev CA (with hostname verification) or trust anything. */
public final class ClientTls {

  public static final String ALPN_PANGATE = "pangate.v1";

  private final Path caCert;
  private volatile SslContext ws;
  private volatile SslContext tcp;

  private ClientTls(Path caCert) {
    this.caCert = caCert;
  }

  public static ClientTls trust(Path caCert) {
    return new ClientTls(caCert);
  }

  public static ClientTls insecure() {
    return new ClientTls(null);
  }

  boolean verifiesHostname() {
    return caCert != null;
  }

  /** Built once per mode and cached: reads the CA file, so never call it on an event loop. */
  SslContext context(boolean tcp) {
    SslContext cached = tcp ? this.tcp : this.ws;
    if (cached == null) {
      synchronized (this) {
        cached = tcp ? this.tcp : this.ws;
        if (cached == null) {
          cached = build(tcp);
          if (tcp) {
            this.tcp = cached;
          } else {
            this.ws = cached;
          }
        }
      }
    }
    return cached;
  }

  private SslContext build(boolean tcp) {
    try {
      SslContextBuilder b = SslContextBuilder.forClient().protocols("TLSv1.3");
      if (caCert == null) {
        b.trustManager(InsecureTrustManagerFactory.INSTANCE);
      } else {
        b.trustManager(caCert.toFile()).endpointIdentificationAlgorithm("HTTPS");
      }
      if (tcp) {
        b.applicationProtocolConfig(new ApplicationProtocolConfig(Protocol.ALPN,
            SelectorFailureBehavior.NO_ADVERTISE, SelectedListenerFailureBehavior.ACCEPT, ALPN_PANGATE));
      }
      return b.build();
    } catch (SSLException e) {
      throw new IllegalStateException("Cannot build client TLS context", e);
    }
  }
}
