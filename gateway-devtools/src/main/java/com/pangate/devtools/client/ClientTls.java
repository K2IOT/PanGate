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

  SslContext context(boolean tcp) {
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
