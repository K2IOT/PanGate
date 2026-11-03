package com.pangate.gateway.auth;

import java.util.List;

/** Verified claims of a connect JWT. {@code principal} is {@code tenantId:userId}. */
public record ConnectClaims(String tenant, String principal, String device, String jti, List<String> topics,
    long expSec) {

  public ConnectClaims {
    topics = List.copyOf(topics);
  }
}
