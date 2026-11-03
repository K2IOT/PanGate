package com.pangate.gateway.protocol;

/** A client broke the protocol; the connection is closed with {@link #code()}. */
public final class ProtocolException extends RuntimeException {

  private final CloseCode code;

  public ProtocolException(CloseCode code, String reason) {
    super(reason, null, false, false);
    this.code = code;
  }

  public CloseCode code() {
    return code;
  }
}
