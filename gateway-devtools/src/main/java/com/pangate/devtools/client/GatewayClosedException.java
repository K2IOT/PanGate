package com.pangate.devtools.client;

/** The connection was closed while waiting for a frame. */
public final class GatewayClosedException extends RuntimeException {

  private final CloseInfo closeInfo;

  public GatewayClosedException(CloseInfo closeInfo) {
    super("gateway closed the connection: " + closeInfo.code() + " " + closeInfo.reason());
    this.closeInfo = closeInfo;
  }

  public CloseInfo closeInfo() {
    return closeInfo;
  }
}
