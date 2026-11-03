package com.pangate.gateway.diagnostics;

import com.pangate.gateway.auth.JwksConfigException;
import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

/** One-line explanation for an invalid JWKS source configuration. */
public final class JwksConfigFailureAnalyzer extends AbstractFailureAnalyzer<JwksConfigException> {

  @Override
  protected FailureAnalysis analyze(Throwable rootFailure, JwksConfigException cause) {
    return new FailureAnalysis(cause.getMessage(),
        "Set exactly one of gateway.auth.jwks-uri or gateway.auth.jwks-file", cause);
  }
}
