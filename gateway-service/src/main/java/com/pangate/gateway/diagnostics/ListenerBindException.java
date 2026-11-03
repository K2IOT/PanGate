package com.pangate.gateway.diagnostics;

/** A data-plane listener could not bind its port. */
public final class ListenerBindException extends RuntimeException {

  private final String listener;
  private final int port;
  private final String envVar;

  public ListenerBindException(String listener, int port, String envVar, Throwable cause) {
    super("Cannot bind " + listener + " listener on port " + port, cause);
    this.listener = listener;
    this.port = port;
    this.envVar = envVar;
  }

  public String listener() {
    return listener;
  }

  public int port() {
    return port;
  }

  public String envVar() {
    return envVar;
  }
}
