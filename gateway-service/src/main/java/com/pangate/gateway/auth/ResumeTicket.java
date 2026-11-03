package com.pangate.gateway.auth;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Contents of a gateway-signed resume ticket (spec §4.7). */
public record ResumeTicket(
    String cell,
    String tenant,
    String principal,
    String device,
    long authAtSec,
    long expSec,
    long authJtiHash,
    List<String> topicPatterns) {

  public static final int MAX_PATTERNS = 8;
  public static final int MAX_PATTERN_BYTES = 64;

  public ResumeTicket {
    topicPatterns = topicPatterns == null ? List.of() : List.copyOf(topicPatterns);
    if (topicPatterns.size() > MAX_PATTERNS) {
      throw new IllegalArgumentException("at most " + MAX_PATTERNS + " topic patterns");
    }
    for (String p : topicPatterns) {
      if (p.getBytes(StandardCharsets.UTF_8).length > MAX_PATTERN_BYTES) {
        throw new IllegalArgumentException("topic pattern longer than " + MAX_PATTERN_BYTES + " bytes");
      }
    }
  }
}
