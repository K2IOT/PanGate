package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.ChatStores.UserConv;
import com.pangate.chat.core.ChatStores.UserRecord;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory stores for core tests (same contracts as ScyllaChatStore / RedisChatCache). */
public final class FakeStores implements ChatStores.ConversationStore, ChatStores.MemberStore, ChatStores.MessageStore,
    ChatStores.DedupStore, ChatStores.MemberCache, ChatStores.UserStore, ChatStores.MarkStore,
    ChatStores.SettingsStore, ChatStores.LastSeenStore, ChatStores.MuteStore, ChatStores.ChannelProgressStore {

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
  public final Map<String, UserRecord> users = new ConcurrentHashMap<>();
  /** convId → userId → marks (CH3 conv_marks; never removed, like the real table). */
  public final Map<String, Map<String, ChatStores.Marks>> convMarks = new ConcurrentHashMap<>();
  /** userId|convId → state time. */
  public final Map<String, Long> stateAt = new ConcurrentHashMap<>();
  public final Map<String, ChatStores.Privacy> privacy = new ConcurrentHashMap<>();
  public final Map<String, Long> lastSeen = new ConcurrentHashMap<>();
  public final Map<String, Long> sent = new ConcurrentHashMap<>();
  /** convId|userId → join order, for membersByJoin. */
  public final Map<String, Long> joined = new ConcurrentHashMap<>();
  private final java.util.concurrent.atomic.AtomicLong joins = new java.util.concurrent.atomic.AtomicLong();
  public int reserveCalls;
  /** Perf 3a: calls of members(), member(), cachedMembers(), get(), lastSeq(). */
  public final java.util.concurrent.atomic.AtomicInteger membersCalls = new java.util.concurrent.atomic.AtomicInteger();
  public final java.util.concurrent.atomic.AtomicInteger memberCalls = new java.util.concurrent.atomic.AtomicInteger();
  public final java.util.concurrent.atomic.AtomicInteger cachedMembersCalls =
      new java.util.concurrent.atomic.AtomicInteger();
  public final java.util.concurrent.atomic.AtomicInteger getCalls = new java.util.concurrent.atomic.AtomicInteger();
  public final java.util.concurrent.atomic.AtomicInteger lastSeqCalls = new java.util.concurrent.atomic.AtomicInteger();

  /** A channel with owner {@code "owner"} and followers {@code f0 .. f<n-1>}. */
  public static FakeStores withChannel(String convId, int followers) {
    FakeStores s = new FakeStores();
    List<Member> ms = new ArrayList<>(followers + 1);
    ms.add(new Member("owner", ChatStores.Role.OWNER));
    for (int i = 0; i < followers; i++) {
      ms.add(new Member("f" + i, ChatStores.Role.MEMBER));
    }
    s.convs.put(convId, new Conversation(convId, com.pangate.contracts.v1.ConversationKind.CONVERSATION_KIND_CHANNEL,
        "c", "owner", 0, true, 0));
    Map<String, Member> byId = new ConcurrentHashMap<>();
    ms.forEach(m -> byId.put(m.userId(), m));
    s.members.put(convId, byId);
    return s;
  }
  public boolean fail;
  /** Fail this many message writes / inbox touches (then succeed). */
  public int failWrites;
  public int failTouches;
  /** Conversation → how many more mark writes of it fail. */
  public final Map<String, Integer> failAdvanceFor = new ConcurrentHashMap<>();
  /** The next N sender-marks writes fail (fan-out retries them). */
  public int failSent;
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
    getCalls.incrementAndGet();
    check();
    Conversation c = convs.get(id);
    return c == null ? Optional.empty() : Optional.of(new Conversation(c.id(), c.kind(), c.title(), c.createdBy(),
        lastSeq.getOrDefault(id, 0L), c.isPublic(), c.memberCount()));
  }
  public final Map<String, String> owners = new ConcurrentHashMap<>();

  @Override public Optional<String> owner(String convId) {
    return Optional.ofNullable(owners.get(convId));
  }

  @Override public synchronized boolean claimOwner(String convId, String expected, String next) {
    if (!java.util.Objects.equals(owners.get(convId), expected)) {
      return false;
    }
    owners.put(convId, next);
    return true;
  }

  @Override public void create(Conversation c, List<Member> ms) {
    check();
    ms.stream().filter(m -> m.role() == ChatStores.Role.OWNER).findFirst().ifPresent(o -> owners.put(c.id(), o.userId()));
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
  @Override public long lastSeq(String convId) { lastSeqCalls.incrementAndGet(); check(); return lastSeq.getOrDefault(convId, 0L); }
  @Override public Optional<String> dm(String a, String b) { return Optional.ofNullable(dms.get(pair(a, b))); }
  @Override public String createDm(String a, String b, String id) { return dms.computeIfAbsent(pair(a, b), k -> id); }
  static String pair(String a, String b) { return a.compareTo(b) <= 0 ? a + "|" + b : b + "|" + a; }

  @Override public List<Member> members(String convId) {
    membersCalls.incrementAndGet();
    check();
    return new ArrayList<>(members.getOrDefault(convId, Map.of()).values());
  }
  @Override public Optional<Member> member(String convId, String userId) {
    memberCalls.incrementAndGet();
    check();
    return Optional.ofNullable(members.getOrDefault(convId, Map.of()).get(userId));
  }
  @Override public void add(String convId, Member m) {
    check();
    members.computeIfAbsent(convId, k -> new ConcurrentHashMap<>()).put(m.userId(), m);
    joined.putIfAbsent(convId + "|" + m.userId(), joins.incrementAndGet());
    Conversation c = convs.get(convId);
    userConvs.computeIfAbsent(m.userId(), k -> new ConcurrentHashMap<>())
        .putIfAbsent(convId, new UserConv(convId, c == null ? null : c.kind(), lastSeq.getOrDefault(convId, 0L),
            lastSeq.getOrDefault(convId, 0L), 0, lastSeq.getOrDefault(convId, 0L)));
  }
  /** Runs once, before the next setRole: a racing request in between. */
  public volatile java.util.function.BiConsumer<String, String> beforeSetRole;

  @Override public void setRole(String convId, String userId, ChatStores.Role role) {
    java.util.function.BiConsumer<String, String> hook = beforeSetRole;
    beforeSetRole = null;
    if (hook != null) {
      hook.accept(convId, userId);
    }
    Member m = members.getOrDefault(convId, Map.of()).get(userId);
    if (m != null) {   // IF EXISTS
      members.get(convId).put(userId, new Member(userId, role));
    }
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
        (o, n) -> n.lastSeq() > o.lastSeq()
            ? new UserConv(convId, o.kind(), n.lastSeq(), o.readSeq(), n.lastMsgAtMs(), o.deliveredSeq()) : o);
    touchState(userId, convId, System.currentTimeMillis());
  }

  // --- CH3 marks: each column keeps its max, as USING TIMESTAMP <mark> does in ScyllaDB ---

  public final Map<String, Long> shownDelivered = new ConcurrentHashMap<>();

  @Override public synchronized void advance(String convId, String userId, long deliveredSeq, long readSeq,
      long shownDeliveredSeq, long atMs) {
    check();
    if (shownDeliveredSeq > 0) {
      shownDelivered.merge(convId + "|" + userId, shownDeliveredSeq, Math::max);
    }
    Integer left = failAdvanceFor.get(convId);
    if (left != null && left > 0) {
      failAdvanceFor.put(convId, left - 1);
      throw new StoreUnavailableException("marks of " + convId + " down", null);
    }
    if (deliveredSeq <= 0 && readSeq <= 0) {
      return;
    }
    convMarks.computeIfAbsent(convId, k -> new ConcurrentHashMap<>()).merge(userId,
        new ChatStores.Marks(Math.max(0, deliveredSeq), Math.max(0, readSeq)),
        (o, n) -> new ChatStores.Marks(Math.max(o.deliveredSeq(), n.deliveredSeq()), Math.max(o.readSeq(), n.readSeq())));
    ChatStores.Marks m = convMarks.get(convId).get(userId);
    userConvs.computeIfAbsent(userId, k -> new ConcurrentHashMap<>()).compute(convId, (k, o) -> o == null
        ? new UserConv(convId, null, 0, m.readSeq(), 0, m.deliveredSeq())
        : new UserConv(convId, o.kind(), o.lastSeq(), Math.max(o.readSeq(), m.readSeq()), o.lastMsgAtMs(),
            Math.max(o.deliveredSeq(), m.deliveredSeq())));
    touchState(userId, convId, atMs);
  }

  @Override public void sent(String convId, String userId, long seq, long atMs) {
    synchronized (this) {
      if (failSent > 0) {
        failSent--;
        throw new StoreUnavailableException("marks down", null);
      }
    }
    advance(convId, userId, seq, seq, seq, atMs);
    sent.merge(convId + "|" + userId, seq, Math::max);
  }

  @Override public long sentSeq(String convId, String userId) {
    return sent.getOrDefault(convId + "|" + userId, 0L);
  }

  @Override public void touchState(String userId, String convId, long atMs) {
    stateAt.merge(userId + "|" + convId, atMs, Math::max);
  }

  @Override public List<ChatStores.MemberMarks> marks(String convId) {
    check();
    return convMarks.getOrDefault(convId, Map.of()).entrySet().stream()
        .map(e -> new ChatStores.MemberMarks(e.getKey(), e.getValue().deliveredSeq(), e.getValue().readSeq(),
            shownDelivered.getOrDefault(convId + "|" + e.getKey(), 0L))).toList();
  }

  @Override public ChatStores.Marks own(String userId, String convId) {
    UserConv u = userConvs.getOrDefault(userId, Map.of()).get(convId);
    return u == null ? ChatStores.Marks.NONE : new ChatStores.Marks(u.deliveredSeq(), u.readSeq());
  }

  @Override public List<ChatStores.UserConvState> changedSince(String userId, long sinceMs, String cursor, int limit) {
    check();
    return userConvs.getOrDefault(userId, Map.of()).values().stream()
        .map(u -> new ChatStores.UserConvState(u.convId(), u.kind(), u.lastSeq(), u.readSeq(), u.deliveredSeq(),
            stateAt.getOrDefault(userId + "|" + u.convId(), 0L)))
        .filter(x -> x.stateAtMs() > sinceMs)
        .filter(x -> cursor == null || cursor.isEmpty() || before(x, cursor))
        .sorted(Comparator.comparingLong(ChatStores.UserConvState::stateAtMs).reversed()
            .thenComparing(ChatStores.UserConvState::convId))
        .limit(limit).toList();
  }

  private static boolean before(ChatStores.UserConvState x, String cursor) {
    int colon = cursor.indexOf(':');
    long at = Long.parseLong(cursor.substring(0, colon));
    String id = cursor.substring(colon + 1);
    return x.stateAtMs() < at || (x.stateAtMs() == at && x.convId().compareTo(id) > 0);
  }

  @Override public ChatStores.Privacy privacy(String userId) {
    return privacy.getOrDefault(userId, ChatStores.Privacy.OPEN);
  }

  @Override public synchronized void updatePrivacy(String userId, Boolean hidePresence, Boolean hideRead,
      Boolean hidePreview, Boolean refuseAdds, long atMs) {
    ChatStores.Privacy cur = privacy(userId);
    privacy.put(userId, new ChatStores.Privacy(hidePresence == null ? cur.hidePresence() : hidePresence,
        hideRead == null ? cur.hideRead() : hideRead, hidePreview == null ? cur.hidePreview() : hidePreview,
        refuseAdds == null ? cur.refuseAdds() : refuseAdds));
  }

  public final Map<String, ChatStores.ChannelProgress> channelProgress = new ConcurrentHashMap<>();

  @Override public void saveChannelProgress(String convId, long convSeq, String lastUserId) {
    check();
    channelProgress.put(convId, new ChatStores.ChannelProgress(convSeq, lastUserId));
  }

  @Override public java.util.Optional<ChatStores.ChannelProgress> loadChannelProgress(String convId) {
    return java.util.Optional.ofNullable(channelProgress.get(convId));
  }

  /** CH4 §3.3: {@code conv|user} → muted until. */
  public final Map<String, Long> mutes = new ConcurrentHashMap<>();

  @Override public void setMute(String convId, String userId, long mutedUntil) {
    if (members.getOrDefault(convId, Map.of()).containsKey(userId)) {
      mutes.put(convId + "|" + userId, mutedUntil);
    }
  }

  @Override public long mutedUntil(String convId, String userId) {
    return mutes.getOrDefault(convId + "|" + userId, 0L);
  }

  @Override public List<ChatStores.MemberMute> membersPage(String convId, String afterUserId, int limit) {
    return members.getOrDefault(convId, Map.of()).keySet().stream().sorted()
        .filter(u -> u.compareTo(afterUserId == null ? "" : afterUserId) > 0).limit(limit)
        .map(u -> new ChatStores.MemberMute(u, mutedUntil(convId, u))).toList();
  }

  @Override public java.util.OptionalLong lastSeen(String userId) {
    Long v = lastSeen.get(userId);
    return v == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(v);
  }

  @Override public void putLastSeen(String userId, long atMs) {
    lastSeen.put(userId, atMs);
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

  @Override public Optional<List<Member>> cachedMembers(String convId) { cachedMembersCalls.incrementAndGet(); return Optional.ofNullable(cache.get(convId)); }
  @Override public void putMembers(String convId, List<Member> ms) { cache.put(convId, List.copyOf(ms)); }
  @Override public boolean evictMembers(String convId) {
    if (evictFails) {
      return false;
    }
    cache.remove(convId);
    return true;
  }

  @Override public List<Member> membersByJoin(String convId) {
    return members(convId).stream()
        .sorted(Comparator.comparingLong((Member m) -> joined.getOrDefault(convId + "|" + m.userId(), 0L))).toList();
  }

  public final java.util.concurrent.atomic.AtomicInteger userReads = new java.util.concurrent.atomic.AtomicInteger();

  @Override public Optional<UserRecord> user(String userId) {
    userReads.incrementAndGet();
    return Optional.ofNullable(users.get(userId));
  }

  @Override public void put(String userId, String status, String displayName, long updatedAtMs) {
    users.put(userId, new UserRecord(userId, status, displayName, updatedAtMs));
  }

  /** Test seeding helper. */
  public boolean upsertIfNewer(String userId, String status, long updatedAtMs) {
    UserRecord prior = users.get(userId);
    if (prior != null && prior.updatedAtMs() > updatedAtMs) {
      return false;
    }
    users.put(userId, new UserRecord(userId, status, updatedAtMs));
    return true;
  }
}
