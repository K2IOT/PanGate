package com.pangate.chat.core;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** The Redis badge hashes of perf 3a (spec §3.3) in memory, with the same rules as the Lua scripts. */
public final class FakeBadges implements ChatStores.BadgeStore {

  static final String TOTAL = "_total";
  static final String BUILT = "_built_at";
  /** Final review I1: the field of a conversation the user left; raises ignore it until a join. */
  static final String LEFT = "L";

  /** user → field → value, like {@code badge:{user}}. */
  public final Map<String, Map<String, String>> hashes = new ConcurrentHashMap<>();
  /** Every call fails. */
  public volatile boolean down;
  /** The next N calls fail, then they work again. */
  public int failNext;
  public final List<String> forgotten = new java.util.concurrent.CopyOnWriteArrayList<>();
  /** Calls that reached this store (failed ones included). */
  public final java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();

  public void lose(String user) {
    hashes.remove(user);
  }

  private synchronized void call() {
    calls.incrementAndGet();
    if (down) {
      throw new StoreUnavailableException("redis down", null);
    }
    if (failNext > 0) {
      failNext--;
      throw new StoreUnavailableException("redis blip", null);
    }
  }

  static long[] parse(String v) {
    if (v == null || v.equals(LEFT)) {
      return new long[] {0, 0};
    }
    int i = v.indexOf(':');
    return new long[] {Long.parseLong(v.substring(0, i)), Long.parseLong(v.substring(i + 1))};
  }

  private static void apply(Map<String, String> h, String conv, long last, long read) {
    if (LEFT.equals(h.get(conv))) {
      return;
    }
    long[] cur = parse(h.get(conv));
    long nl = Math.max(cur[0], last);
    long nr = Math.max(cur[1], read);
    long delta = Math.max(0, nl - nr) - Math.max(0, cur[0] - cur[1]);
    h.put(conv, nl + ":" + nr);
    h.put(TOTAL, Long.toString(Long.parseLong(h.getOrDefault(TOTAL, "0")) + delta));
  }

  @Override
  public synchronized long totalWith(String user, String convId, long seq, long builtAfterMs) {
    call();
    Map<String, String> h = hashes.get(user);
    if (h == null || Long.parseLong(h.getOrDefault(BUILT, "0")) < builtAfterMs) {
      return -1;
    }
    long total = Long.parseLong(h.getOrDefault(TOTAL, "0"));
    if (convId == null || LEFT.equals(h.get(convId))) {
      return total;
    }
    if (!h.containsKey(convId)) {
      return total + (seq > 0 ? 1 : 0);   // not known yet (or left before a rebuild): the message alone, not kept
    }
    apply(h, convId, seq, 0);
    return Long.parseLong(h.getOrDefault(TOTAL, "0"));
  }

  @Override
  public synchronized void rebuild(String user, List<ChatStores.BadgeRow> snapshot, long nowMs) {
    call();
    Map<String, String> h = hashes.computeIfAbsent(user, k -> new ConcurrentHashMap<>());
    Set<String> keep = new HashSet<>();
    for (ChatStores.BadgeRow r : snapshot) {
      keep.add(r.convId());
      long[] cur = parse(h.get(r.convId()));
      h.put(r.convId(), Math.max(cur[0], r.lastSeq()) + ":" + Math.max(cur[1], r.readSeq()));
    }
    h.replaceAll((k, v) -> k.startsWith("_") || keep.contains(k) ? v : LEFT);
    long total = 0;
    for (Map.Entry<String, String> e : h.entrySet()) {
      if (!e.getKey().startsWith("_")) {
        long[] v = parse(e.getValue());
        total += Math.max(0, v[0] - v[1]);
      }
    }
    h.put(TOTAL, Long.toString(total));
    h.put(BUILT, Long.toString(nowMs));
  }

  @Override
  public synchronized void raise(String user, String convId, long last, long read) {
    call();
    Map<String, String> h = hashes.get(user);
    if (h != null) {
      apply(h, convId, last, read);
    }
  }

  @Override
  public synchronized void raiseAll(List<String> users, String convId, long last) {
    call();
    for (String u : users) {
      Map<String, String> h = hashes.get(u);
      if (h != null) {
        apply(h, convId, last, 0);
      }
    }
  }

  @Override
  public synchronized void join(String user, String convId, long last) {
    call();
    Map<String, String> h = hashes.get(user);
    if (h != null) {
      if (LEFT.equals(h.get(convId))) {
        h.remove(convId);
      }
      apply(h, convId, last, last);
    }
  }

  @Override
  public synchronized void drop(String user, String convId) {
    call();
    Map<String, String> h = hashes.get(user);
    if (h != null) {
      long[] cur = parse(h.put(convId, LEFT));
      h.put(TOTAL, Long.toString(Long.parseLong(h.getOrDefault(TOTAL, "0")) - Math.max(0, cur[0] - cur[1])));
    }
  }

  @Override
  public synchronized void forget(Collection<String> users) {
    call();
    for (String u : users) {
      hashes.remove(u);
      forgotten.add(u);
    }
  }
}
