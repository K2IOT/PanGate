package com.pangate.gateway.diagnostics;

import java.time.Duration;

/** A dependency (Redis, Kafka) stayed unreachable for the whole startup dependency timeout (spec §3.7, R4). */
public final class DependencyUnavailableException extends RuntimeException {

  private final String dependency;
  private final String target;
  private final Duration waited;
  private final String envVar;

  public DependencyUnavailableException(String dependency, String target, Duration waited, String envVar,
      Throwable cause) {
    super(dependency + " " + target + " is not reachable after " + waited.toSeconds() + " s", cause);
    this.dependency = dependency;
    this.target = target;
    this.waited = waited;
    this.envVar = envVar;
  }

  public String dependency() {
    return dependency;
  }

  public String target() {
    return target;
  }

  public Duration waited() {
    return waited;
  }

  public String envVar() {
    return envVar;
  }
}
