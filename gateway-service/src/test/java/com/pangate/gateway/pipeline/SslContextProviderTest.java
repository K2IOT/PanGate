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

  private final io.micrometer.core.instrument.simple.SimpleMeterRegistry meters =
      new io.micrometer.core.instrument.simple.SimpleMeterRegistry();

  private SslContextProvider provider(Path d) {
    java.util.concurrent.ScheduledExecutorService now = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
    return new SslContextProvider(new GatewayProperties.Tls(d.resolve("gateway.crt"), d.resolve("gateway.key"),
        java.time.Duration.ofSeconds(30)), now, java.time.Duration.ZERO,
        new com.pangate.gateway.metrics.GatewayMetrics(meters));
  }

  private double reloads(String result) {
    var c = meters.find("gw.tls.reloads").tag("result", result).counter();
    return c == null ? 0 : c.count();
  }

  private static void bumpMtime(Path d) throws Exception {
    for (String f : new String[] {"gateway.crt", "gateway.key"}) {
      java.io.File file = d.resolve(f).toFile();
      file.setLastModified(file.lastModified() + 2000);
    }
  }

  @Test
  void unchangedFilesDoNotRebuild(@TempDir Path d) {
    DevSecretsGenerator.ensure(d);
    SslContextProvider p = provider(d);
    io.netty.handler.ssl.SslContext before = p.ws();

    assertThat(p.reloadIfChanged()).isFalse();
    assertThat(p.ws()).isSameAs(before);
  }

  @Test
  void replacedCertIsServedToNewConnections(@TempDir Path d) throws Exception {
    DevSecretsGenerator.ensure(d);
    SslContextProvider p = provider(d);
    io.netty.handler.ssl.SslContext before = p.ws();
    io.netty.handler.ssl.SslContext beforeTcp = p.tcp();
    java.util.List<io.netty.handler.ssl.SslContext> built = new java.util.ArrayList<>();
    p.onContextsBuilt(built::add);

    DevSecretsGenerator.reissueGatewayCert(d);
    bumpMtime(d);

    assertThat(p.reloadIfChanged()).isTrue();
    assertThat(p.ws()).isNotSameAs(before);
    assertThat(p.tcp()).isNotSameAs(beforeTcp);
    assertThat(built).containsExactly(p.ws(), p.tcp());
    SSLEngine engine = p.ws().newEngine(ByteBufAllocator.DEFAULT);
    ReferenceCountUtil.release(engine);
    if (before instanceof io.netty.util.ReferenceCounted rc) {
      org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(2)).until(() -> rc.refCnt() == 0);
    }
    assertThat(reloads("ok")).isEqualTo(1);
  }

  @Test
  void brokenReplacementKeepsCurrentContext(@TempDir Path d) throws Exception {
    DevSecretsGenerator.ensure(d);
    SslContextProvider p = provider(d);
    io.netty.handler.ssl.SslContext before = p.ws();
    String goodKey = java.nio.file.Files.readString(d.resolve("gateway.key"));

    java.nio.file.Files.writeString(d.resolve("gateway.key"), "garbage");
    bumpMtime(d);

    assertThat(p.reloadIfChanged()).isFalse();
    assertThat(p.ws()).isSameAs(before);
    assertThat(reloads("failed")).isEqualTo(1);

    java.nio.file.Files.writeString(d.resolve("gateway.key"), goodKey);
    bumpMtime(d);
    assertThat(p.reloadIfChanged()).isTrue();
  }

  @Test
  void loadsDevCertificateWithTls13Only(@TempDir Path d) {
    DevSecretsGenerator.ensure(d);
    SslContextProvider p = provider(d);

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
    SslContextProvider p = provider(d);

    assertThat(p.tcp().applicationProtocolNegotiator().protocols()).containsExactly("pangate.v1");
    assertThat(p.ws().applicationProtocolNegotiator().protocols()).containsExactly("http/1.1");
  }

  @Test
  void missingCertFileThrowsSecretNotFound(@TempDir Path d) {
    assertThatThrownBy(() -> provider(d))
        .isInstanceOf(SecretNotFoundException.class)
        .hasMessageContaining("gateway.crt");
  }
}
