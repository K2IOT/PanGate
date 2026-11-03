package com.pangate.gateway.auth;

/** One HMAC-SHA256 resume-ticket key (32 bytes) and its id. */
public record TicketKey(int kid, byte[] key) {

  public TicketKey {
    if (key.length != 32) {
      throw new IllegalArgumentException("ticket key " + kid + " must be 32 bytes, got " + key.length);
    }
    key = key.clone();
  }
}
