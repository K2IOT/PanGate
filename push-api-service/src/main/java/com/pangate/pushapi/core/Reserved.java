package com.pangate.pushapi.core;

/** msg_id and seq values reserved for one idempotency key; text form {@code pending|msgId|seq|epoch|topicSeq}. */
public record Reserved(String msgId, long seq, int seqEpoch, long topicSeq) {

  static final String PENDING = "pending|";

  String encode() {
    return PENDING + msgId + "|" + seq + "|" + seqEpoch + "|" + topicSeq;
  }

  static Reserved decode(String v) {
    String[] p = v.substring(PENDING.length()).split("\\|", -1);
    if (p.length != 4) {
      throw new IllegalArgumentException("not a pending value");
    }
    return new Reserved(p[0], Long.parseLong(p[1]), Integer.parseInt(p[2]), Long.parseLong(p[3]));
  }
}
