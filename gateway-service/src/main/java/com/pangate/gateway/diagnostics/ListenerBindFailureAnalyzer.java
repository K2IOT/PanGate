package com.pangate.gateway.diagnostics;

import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

/** One-line explanation for a data-plane port that is already in use. */
public final class ListenerBindFailureAnalyzer extends AbstractFailureAnalyzer<ListenerBindException> {

  @Override
  protected FailureAnalysis analyze(Throwable rootFailure, ListenerBindException cause) {
    return new FailureAnalysis("Port " + cause.port() + " (" + cause.listener() + ") is already in use",
        "Stop the other process or set " + cause.envVar(), cause);
  }
}
