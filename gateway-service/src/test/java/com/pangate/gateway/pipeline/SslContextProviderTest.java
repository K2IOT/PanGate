package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.dev.DevSecretsGenerator;
import com.pangate.starter.secrets.SecretNotFoundException;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.nio.file.Path;
import javax.net.ssl.SSLEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SslContextProviderTest {

  @Test
  void loadsDevCertificateWithTls13Only(@TempDir Path d) {
    DevSecretsGenerator.ensure(d);
    SslContextProvider p = new SslContextProvider(new GatewayProperties.Tls(d.resolve("gateway.crt"), d.resolve("gateway.key")));

    SSLEngine engine = p.ws().newEngine(ByteBufAllocator.DEFAULT);
    try {
      // OpenSSL engines also report the "SSLv2Hello" pseudo-protocol (ClientHello format), not a protocol version.
      assertThat(engine.getEnabledProtocols()).filteredOn(proto -> !proto.equals("SSLv2Hello")).containsExactly("TLSv1.3");
      assertThat(p.ws().isServer()).isTrue();
    } finally {
      ReferenceCountUtil.release(engine);
    }
  }

  @Test
  void tcpContextAdvertisesPangateAlpn(@TempDir Path d) {
    DevSecretsGenerator.ensure(d);
    SslContextProvider p = new SslContextProvider(new GatewayProperties.Tls(d.resolve("gateway.crt"), d.resolve("gateway.key")));

    assertThat(p.tcp().applicationProtocolNegotiator().protocols()).containsExactly("pangate.v1");
    assertThat(p.ws().applicationProtocolNegotiator().protocols()).containsExactly("http/1.1");
  }

  @Test
  void missingCertFileThrowsSecretNotFound(@TempDir Path d) {
    assertThatThrownBy(() -> new SslContextProvider(new GatewayProperties.Tls(d.resolve("gateway.crt"), d.resolve("gateway.key"))))
        .isInstanceOf(SecretNotFoundException.class)
        .hasMessageContaining("gateway.crt");
  }
}
