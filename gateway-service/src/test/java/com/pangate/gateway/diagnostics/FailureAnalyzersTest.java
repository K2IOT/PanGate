package com.pangate.gateway.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.auth.JwksConfigException;
import com.pangate.starter.secrets.SecretNotFoundException;
import java.net.BindException;
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
}
