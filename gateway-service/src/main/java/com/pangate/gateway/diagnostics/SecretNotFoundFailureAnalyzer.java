package com.pangate.gateway.diagnostics;

import com.pangate.starter.secrets.SecretNotFoundException;
import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

/** One-line explanation for a missing secret file. */
public final class SecretNotFoundFailureAnalyzer extends AbstractFailureAnalyzer<SecretNotFoundException> {

  @Override
  protected FailureAnalysis analyze(Throwable rootFailure, SecretNotFoundException cause) {
    return new FailureAnalysis(cause.getMessage(),
        "Run with profile local (auto-generates dev secrets) or provide the file", cause);
  }
}
