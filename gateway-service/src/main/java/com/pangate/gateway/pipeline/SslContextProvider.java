package com.pangate.gateway.pipeline;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.starter.secrets.SecretNotFoundException;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Server TLS contexts (spec §7.7): TLS 1.3 only, BoringSSL; WS negotiates http/1.1, TCP requires ALPN pangate.v1. */
public final class SslContextProvider {

  private static final Logger log = LoggerFactory.getLogger(SslContextProvider.class);

  private final SslContext ws;
  private final SslContext tcp;

  public SslContextProvider(GatewayProperties.Tls tls) {
    require(tls.certFile());
    require(tls.keyFile());
    SslProvider provider = SslProvider.OPENSSL_REFCNT;
    if (!OpenSsl.isAvailable()) {
      log.warn("BoringSSL (netty-tcnative) unavailable, falling back to the JDK TLS provider: {}",
          String.valueOf(OpenSsl.unavailabilityCause()));
      provider = SslProvider.JDK;
    }
    this.ws = build(tls, provider, "http/1.1");
    this.tcp = build(tls, provider, HelloAuthHandler.ALPN_PANGATE);
    log.info("TLS contexts loaded from {} using {}", tls.certFile(), provider);
  }

  private static void require(Path file) {
    if (!Files.isReadable(file)) {
      throw new SecretNotFoundException(file.getFileName().toString(), file.toString(), null);
    }
  }

  private static SslContext build(GatewayProperties.Tls tls, SslProvider provider, String alpn) {
    try {
      return SslContextBuilder.forServer(tls.certFile().toFile(), tls.keyFile().toFile())
          .sslProvider(provider)
          .protocols("TLSv1.3")
          .applicationProtocolConfig(new ApplicationProtocolConfig(Protocol.ALPN,
              SelectorFailureBehavior.NO_ADVERTISE, SelectedListenerFailureBehavior.ACCEPT, alpn))
          .build();
    } catch (SSLException e) {
      throw new IllegalStateException("Cannot build TLS context from " + tls.certFile() + ": " + e.getMessage(), e);
    }
  }

  public SslContext ws() {
    return ws;
  }

  public SslContext tcp() {
    return tcp;
  }
}
