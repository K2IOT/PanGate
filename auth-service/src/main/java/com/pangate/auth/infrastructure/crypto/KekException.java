package com.pangate.auth.infrastructure.crypto;

/** The KEK secret is missing or not 32 bytes; explained at startup by {@code KekFailureAnalyzer}. */
public final class KekException extends IllegalStateException {

  private final String secretName;
  private final String location;
  private final String problem;

  public KekException(String secretName, String location, String problem, Throwable cause) {
    super("Secret " + secretName + " at " + location + ": " + problem, cause);
    this.secretName = secretName;
    this.location = location;
    this.problem = problem;
  }

  public String secretName() {
    return secretName;
  }

  public String location() {
    return location;
  }

  public String problem() {
    return problem;
  }
}
