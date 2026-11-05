package com.pangate.gateway.diagnostics;

import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

public final class MissingTopicsFailureAnalyzer extends AbstractFailureAnalyzer<MissingTopicsException> {

  @Override
  protected FailureAnalysis analyze(Throwable root, MissingTopicsException cause) {
    return new FailureAnalysis(cause.getMessage(),
        "Create them or set gateway.kafka.create-topics=true (local/dev only)", cause);
  }
}
