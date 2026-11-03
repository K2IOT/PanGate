package com.pangate.devtools.jwt;

import java.time.Duration;
import java.util.List;

/** What a connect token is minted for. */
public record TokenRequest(String tenant, String principal, String device, List<String> topics, Duration ttl) {

  public TokenRequest {
    topics = topics == null ? List.of() : List.copyOf(topics);
  }
}
