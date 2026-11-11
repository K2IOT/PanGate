package com.pangate.pushapi.core;

import java.util.Optional;

/**
 * msg_id and seq values reserved for one idempotency key; text form
 * {@code pending|msgId|seq|epoch|topicSeq|request}, where {@code request} names what they were reserved for (kind,
 * class and target), so a retry with the same key but another request never reuses them.
 */
public record Reserved(String msgId, long seq, int seqEpoch, long topicSeq) {

  static final String PENDING = "pending|";

  String encode(String request) {
    return PENDING + msgId + "|" + seq + "|" + seqEpoch + "|" + topicSeq + "|" + request;
  }

  /** The values in {@code v} if they were reserved for {@code request}. */
  static Optional<Reserved> decodeFor(String v, String request) {
    if (!v.startsWith(PENDING)) {
      return Optional.empty();
    }
    String[] p = v.substring(PENDING.length()).split("\\|", 5);
    if (p.length != 5 || !p[4].equals(request)) {
      return Optional.empty();
    }
    try {
      return Optional.of(new Reserved(p[0], Long.parseLong(p[1]), Integer.parseInt(p[2]), Long.parseLong(p[3])));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }
}
