package com.pangate.gateway.downstream;

import java.util.List;

/** Topic namespace and JWT topic-pattern checks (spec §5.7): {@code *} matches any run of characters, colons too. */
public final class TopicPatterns {

  private TopicPatterns() {}

  public static boolean inTenant(String tenantId, String topic) {
    return topic.length() > tenantId.length() + 1 && topic.startsWith(tenantId) && topic.charAt(tenantId.length()) == ':';
  }

  public static boolean allowed(List<String> patterns, String topic) {
    for (String p : patterns) {
      if (glob(p, topic)) {
        return true;
      }
    }
    return false;
  }

  /** Iterative wildcard match with backtracking to the last star: O(p·t) worst case, patterns are ≤ 64 bytes. */
  static boolean glob(String p, String t) {
    int pi = 0;
    int ti = 0;
    int star = -1;
    int mark = 0;
    while (ti < t.length()) {
      if (pi < p.length() && p.charAt(pi) == '*') {
        star = pi++;
        mark = ti;
      } else if (pi < p.length() && p.charAt(pi) == t.charAt(ti)) {
        pi++;
        ti++;
      } else if (star >= 0) {
        pi = star + 1;
        ti = ++mark;
      } else {
        return false;
      }
    }
    while (pi < p.length() && p.charAt(pi) == '*') {
      pi++;
    }
    return pi == p.length();
  }
}
