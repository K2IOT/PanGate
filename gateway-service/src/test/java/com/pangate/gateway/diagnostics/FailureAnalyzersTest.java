package com.pangate.gateway.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.auth.JwksConfigException;
import com.pangate.starter.secrets.SecretNotFoundException;
import java.net.BindException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.diagnostics.FailureAnalysis;

class FailureAnalyzersTest {

  @Test
  void bindFailureAnalysisNamesPortAndEnvVar() {
    FailureAnalysis a = new ListenerBindFailureAnalyzer().analyze(new IllegalStateException(
        new ListenerBindException("ws", 8443, "GATEWAY_LISTENERS_WS_PORT", new BindException("in use"))));

    assertThat(a.getDescription()).isEqualTo("Port 8443 (ws) is already in use");
    assertThat(a.getAction()).isEqualTo("Stop the other process or set GATEWAY_LISTENERS_WS_PORT");
  }

  @Test
  void missingSecretAnalysisNamesPath() {
    FailureAnalysis a = new SecretNotFoundFailureAnalyzer().analyze(
        new SecretNotFoundException("gateway.key", "/etc/pangate/tls/gateway.key", null));

    assertThat(a.getDescription()).isEqualTo("Secret 'gateway.key' not found at /etc/pangate/tls/gateway.key");
    assertThat(a.getAction()).isEqualTo("Run with profile local (auto-generates dev secrets) or provide the file");
  }

  @Test
  void jwksConfigAnalysis() {
    FailureAnalysis a = new JwksConfigFailureAnalyzer().analyze(new JwksConfigException("both set"));

    assertThat(a.getDescription()).isEqualTo("both set");
    assertThat(a.getAction()).isEqualTo("Set exactly one of gateway.auth.jwks-uri or gateway.auth.jwks-file");
  }
  @Test
  void dependencyUnavailableNamesTargetAndFix() {
    FailureAnalysis a = new DependencyFailureAnalyzer().analyze(new IllegalStateException(
        new DependencyUnavailableException("Kafka", "localhost:9092", Duration.ofSeconds(30),
            "GATEWAY_KAFKA_BOOTSTRAP", null)));

    assertThat(a.getDescription()).isEqualTo("Kafka localhost:9092 is not reachable after 30 s");
    assertThat(a.getAction()).isEqualTo("Run 'make infra-up' or set GATEWAY_KAFKA_BOOTSTRAP");
  }

  @Test
  void missingTopicsListsThem() {
    FailureAnalysis a = new MissingTopicsFailureAnalyzer().analyze(
        new MissingTopicsException(List.of("c1.gw.lifecycle", "gw.control")));

    assertThat(a.getDescription()).isEqualTo("Kafka topics missing: c1.gw.lifecycle, gw.control");
    assertThat(a.getAction()).isEqualTo("Create them or set gateway.kafka.create-topics=true (local/dev only)");
  }
}
