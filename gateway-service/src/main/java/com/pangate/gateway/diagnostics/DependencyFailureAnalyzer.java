package com.pangate.gateway.diagnostics;

import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

/** One line saying which dependency is unreachable and how to fix it (spec §9.1 R4). */
public final class DependencyFailureAnalyzer extends AbstractFailureAnalyzer<DependencyUnavailableException> {

  @Override
  protected FailureAnalysis analyze(Throwable root, DependencyUnavailableException cause) {
    return new FailureAnalysis(cause.getMessage(), "Run 'make infra-up' or set " + cause.envVar(), cause);
  }
}
