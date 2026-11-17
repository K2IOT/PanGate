package com.pangate.chat.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/** A conversation's {@code last_seq}, cached 5 s per pod: the bound marks are clamped to (CH3 spec §3.3). */
public final class LastSeqCache {

  static final long TTL_MS = 5_000;
  static final int MAX = 100_000;

  private record Entry(long lastSeq, long until) {}

  private final ChatStores.ConversationStore conversations;
  private final LongSupplier monoMs;
  private final Map<String, Entry> byConv = new ConcurrentHashMap<>();

  public LastSeqCache(ChatStores.ConversationStore conversations, LongSupplier monoMs) {
    this.conversations = conversations;
    this.monoMs = monoMs;
  }

  public long lastSeq(String convId) {
    return lastSeq(convId, 0);
  }

  /**
   * The cached value when it covers {@code wanted}; otherwise a fresh read (final review C1: a mark for a message
   * newer than the cache must not be clamped back).
   */
  public long lastSeq(String convId, long wanted) {
    long now = monoMs.getAsLong();
    Entry e = byConv.get(convId);
    if (e != null && now < e.until() && e.lastSeq() >= wanted) {
      return e.lastSeq();
    }
    if (byConv.size() >= MAX) {
      byConv.clear();
    }
    long v = conversations.lastSeq(convId);
    byConv.put(convId, new Entry(v, now + TTL_MS));
    return v;
  }
}
