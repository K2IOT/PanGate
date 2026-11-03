package com.pangate.gateway.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

class GatewayPropertiesTest {

  @Configuration
  @EnableConfigurationProperties(GatewayProperties.class)
  static class Config {}

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
      .withInitializer(new ConfigDataApplicationContextInitializer())
      .withUserConfiguration(Config.class);

  @Test
  void prodDefaultsMatchSpec() {
    runner.run(ctx -> {
      GatewayProperties p = ctx.getBean(GatewayProperties.class);
      GatewayProperties.Limits l = p.limits();
      assertThat(l.hardCap()).isEqualTo(700_000);
      assertThat(l.softCap()).isEqualTo(550_000);
      assertThat(l.perIp()).isEqualTo(200);
      assertThat(l.frameMax()).isEqualTo(65_536);
      assertThat(l.helloMax()).isEqualTo(8_192);
      assertThat(l.helloTimeout()).isEqualTo(Duration.ofSeconds(5));
      assertThat(l.handshakeTimeout()).isEqualTo(Duration.ofSeconds(10));
      assertThat(l.heartbeatInterval()).isEqualTo(Duration.ofSeconds(30));
      assertThat(l.sessionTtl()).isEqualTo(Duration.ofHours(24));
      assertThat(l.handshakesPerPrincipalPerMin()).isEqualTo(20);
      assertThat(p.admission().rate()).isEqualTo(5_000);
      assertThat(p.admission().burst()).isEqualTo(10_000);
      assertThat(p.admission().rampFrom()).isEqualTo(500);
      assertThat(p.admission().rampDuration()).isEqualTo(Duration.ofMinutes(2));
      assertThat(p.auth().audience()).isEqualTo("pangate-gw");
      assertThat(p.auth().clockSkew()).isEqualTo(Duration.ofSeconds(30));
      assertThat(p.auth().jwksRefresh()).isEqualTo(Duration.ofMinutes(5));
      assertThat(p.listeners().ws().port()).isEqualTo(8443);
      assertThat(p.listeners().ws().path()).isEqualTo("/v1/ws");
      assertThat(p.listeners().ws().acceptors()).isEqualTo(4);
      assertThat(p.listeners().tcp().port()).isEqualTo(8883);
      assertThat(p.dev().bootstrapSecrets()).isFalse();
      assertThat(GatewayLimits.from(p).idleTimeoutSec()).isEqualTo(90);
      assertThat(ctx.getEnvironment().getProperty("server.port")).isEqualTo("8081");
      assertThat(ctx.getEnvironment().getProperty("management.endpoint.health.group.readiness.additional-path"))
          .isEqualTo("server:/ready");
      assertThat(ctx.getEnvironment().getProperty("management.endpoint.health.group.liveness.additional-path"))
          .isEqualTo("server:/live");
    });
  }

  @Test
  void localProfileOverrides() {
    runner.withPropertyValues("spring.profiles.active=local").run(ctx -> {
      GatewayProperties p = ctx.getBean(GatewayProperties.class);
      Path devDir = Path.of(System.getProperty("user.home"), ".pangate-dev");
      assertThat(p.nodeId()).isEqualTo("gateway-local");
      assertThat(p.limits().hardCap()).isEqualTo(100_000);
      assertThat(p.limits().softCap()).isEqualTo(80_000);
      assertThat(p.limits().perIp()).isEqualTo(100_000);
      assertThat(p.dev().bootstrapSecrets()).isTrue();
      assertThat(p.dev().dir()).isEqualTo(devDir);
      assertThat(p.auth().jwksFile()).isEqualTo(devDir.resolve("jwks.json"));
      assertThat(p.auth().jwksUri()).isEmpty();
      assertThat(p.tls().certFile()).isEqualTo(devDir.resolve("gateway.crt"));
      assertThat(ctx.getEnvironment().getProperty("pangate.secrets.dir")).isEqualTo(devDir.toString());
    });
  }

  @Test
  void devProfileUsesIssuerJwks() {
    runner.withPropertyValues("spring.profiles.active=dev").run(ctx -> {
      GatewayProperties p = ctx.getBean(GatewayProperties.class);
      assertThat(p.auth().jwksUri()).isEqualTo("http://issuer:8090/.well-known/jwks.json");
      assertThat(p.auth().jwksFile()).isNull();
      assertThat(p.limits().hardCap()).isEqualTo(200_000);
      assertThat(p.nodeId()).isEqualTo("gateway-dev-1");
      assertThat(p.dev().dir()).isEqualTo(Path.of("/dev-secrets"));
    });
  }
}
