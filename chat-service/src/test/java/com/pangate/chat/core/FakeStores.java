package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.ChatStores.UserConv;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory stores for core tests (same contracts as ScyllaChatStore / RedisChatCache). */
public final class FakeStores implements ChatStores.ConversationStore, ChatStores.MemberStore, ChatStores.MessageStore,
    ChatStores.DedupStore, ChatStores.MemberCache {

  public static final com.pangate.chat.config.ChatProperties.Limits LIMITS =
      new com.pangate.chat.config.ChatProperties.Limits(20, 40, 50, 100, 500, 4096, 50);

  public final Map<String, Conversation> convs = new ConcurrentHashMap<>();
  public final Map<String, Map<String, Member>> members = new ConcurrentHashMap<>();
  public final Map<String, Map<Long, StoredMessage>> messages = new ConcurrentHashMap<>();
  public final Map<String, StoredMessage> dedup = new ConcurrentHashMap<>();
  public final Map<String, Map<String, UserConv>> userConvs = new ConcurrentHashMap<>();
  public final Map<String, Long> lastSeq = new ConcurrentHashMap<>();
  public final Map<String, String> dms = new ConcurrentHashMap<>();
  public final Map<String, List<Member>> cache = new ConcurrentHashMap<>();
  public int reserveCalls;
  public boolean fail;
  /** Fail this many message writes / inbox touches (then succeed). */
  public int failWrites;
  public int failTouches;
  public boolean evictFails;
  /** A message with this clientMsgId never writes: a deterministic (non-transient) failure. */
  public volatile String poisonClientMsgId;
  public int touches;

  void check() {
    if (fail) {
      throw new StoreUnavailableException("down", null);
    }
  }

  @Override public Optional<Conversation> get(String id) {
    check();
    Conversation c = convs.get(id);
    return c == null ? Optional.empty() : Optional.of(new Conversation(c.id(), c.kind(), c.title(), c.createdBy(),
        lastSeq.getOrDefault(id, 0L), c.isPublic(), c.memberCount()));
  }
  @Override public void create(Conversation c, List<Member> ms) {
    check();
    convs.put(c.id(), c);
    lastSeq.put(c.id(), 0L);
    ms.forEach(m -> add(c.id(), m));
  }
  @Override public synchronized boolean reserveSeq(String convId, long known, int n) {
    check();
    reserveCalls++;
    if (lastSeq.getOrDefault(convId, 0L) != known) {
      return false;
    }
    lastSeq.put(convId, known + n);
    return true;
  }
  @Override public long lastSeq(String convId) { check(); return lastSeq.getOrDefault(convId, 0L); }
  @Override public Optional<String> dm(String a, String b) { return Optional.ofNullable(dms.get(pair(a, b))); }
  @Override public String createDm(String a, String b, String id) { return dms.computeIfAbsent(pair(a, b), k -> id); }
  static String pair(String a, String b) { return a.compareTo(b) <= 0 ? a + "|" + b : b + "|" + a; }

  @Override public List<Member> members(String convId) {
    check();
    return new ArrayList<>(members.getOrDefault(convId, Map.of()).values());
  }
  @Override public Optional<Member> member(String convId, String userId) {
    check();
    return Optional.ofNullable(members.getOrDefault(convId, Map.of()).get(userId));
  }
  @Override public void add(String convId, Member m) {
    check();
    members.computeIfAbsent(convId, k -> new ConcurrentHashMap<>()).put(m.userId(), m);
    Conversation c = convs.get(convId);
    userConvs.computeIfAbsent(m.userId(), k -> new ConcurrentHashMap<>())
        .putIfAbsent(convId, new UserConv(convId, c == null ? null : c.kind(), 0, 0, 0));
  }
  @Override public void remove(String convId, String userId) {
    check();
    members.getOrDefault(convId, Map.of()).remove(userId);
    userConvs.getOrDefault(userId, Map.of()).remove(convId);
  }
  @Override public List<UserConv> userConvs(String userId, String cursor, int limit) {
    return userConvs.getOrDefault(userId, Map.of()).values().stream()
        .sorted(Comparator.comparingLong(UserConv::lastMsgAtMs).reversed()).limit(limit).toList();
  }
  @Override public void touchUserConv(String userId, String convId, long seq, long atMs) {
    synchronized (this) {
      if (failTouches > 0) {
        failTouches--;
        throw new StoreUnavailableException("touch down", null);
      }
      touches++;
    }
    userConvs.computeIfAbsent(userId, k -> new ConcurrentHashMap<>()).merge(convId,
        new UserConv(convId, null, seq, 0, atMs),
        (o, n) -> n.lastSeq() > o.lastSeq() ? new UserConv(convId, o.kind(), n.lastSeq(), o.readSeq(), n.lastMsgAtMs()) : o);
  }

  @Override public void write(StoredMessage m) {
    check();
    if (m.clientMsgId().equals(poisonClientMsgId)) {
      throw new IllegalArgumentException("cannot store " + m.clientMsgId());
    }
    synchronized (this) {
      if (failWrites > 0) {
        failWrites--;
        throw new StoreUnavailableException("write timed out", null);
      }
    }
    messages.computeIfAbsent(m.convId(), k -> new ConcurrentHashMap<>()).put(m.convSeq(), m);
  }
  @Override public List<StoredMessage> before(String convId, long beforeSeq, int limit) {
    return messages.getOrDefault(convId, Map.of()).values().stream().filter(m -> m.convSeq() < beforeSeq)
        .sorted(Comparator.comparingLong(StoredMessage::convSeq).reversed()).limit(limit).toList();
  }
  @Override public List<StoredMessage> after(String convId, long afterSeq, int limit) {
    return messages.getOrDefault(convId, Map.of()).values().stream().filter(m -> m.convSeq() > afterSeq)
        .sorted(Comparator.comparingLong(StoredMessage::convSeq)).limit(limit).toList();
  }

  static String dk(String c, String s, String k) { return c + "|" + s + "|" + k; }
  @Override public Optional<StoredMessage> find(String c, String s, String k) {
    check();
    return Optional.ofNullable(dedup.get(dk(c, s, k)));
  }
  @Override public void put(StoredMessage m) { check(); dedup.put(dk(m.convId(), m.senderId(), m.clientMsgId()), m); }

  @Override public Optional<List<Member>> cachedMembers(String convId) { return Optional.ofNullable(cache.get(convId)); }
  @Override public void putMembers(String convId, List<Member> ms) { cache.put(convId, List.copyOf(ms)); }
  @Override public boolean evictMembers(String convId) {
    if (evictFails) {
      return false;
    }
    cache.remove(convId);
    return true;
  }
}
