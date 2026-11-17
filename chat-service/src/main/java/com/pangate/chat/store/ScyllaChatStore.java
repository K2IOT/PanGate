package com.pangate.chat.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.pangate.chat.core.ChatStores;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.ChatStores.UserConv;
import com.pangate.chat.core.StoreUnavailableException;
import com.pangate.contracts.v1.ConversationKind;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * The chat tables on ScyllaDB (chat spec §5). Messages live in partitions {@code (conv_id, bucket)} with
 * {@code bucket = conv_seq / 10 000}; seqs of a conversation are contiguous, so one page of {@code limit} messages
 * touches at most the buckets between its two ends.
 */
public final class ScyllaChatStore implements ChatStores.ConversationStore, ChatStores.MemberStore, ChatStores.UserStore,
    ChatStores.MarkStore, ChatStores.SettingsStore, ChatStores.LastSeenStore,
    ChatStores.MessageStore, ChatStores.DedupStore {

  public static final long BUCKET = 10_000;

  private final CqlSession s;
  private final PreparedStatement insertConv;
  private final PreparedStatement selectConv;
  private final PreparedStatement reserve;
  private final PreparedStatement selectLastSeq;
  private final PreparedStatement insertMember;
  private final PreparedStatement deleteMember;
  private final PreparedStatement selectMembers;
  private final PreparedStatement selectMember;
  private final PreparedStatement joinUserConv;
  private final PreparedStatement leaveUserConv;
  private final PreparedStatement touchUserConv;
  private final PreparedStatement selectUserConvs;
  private final PreparedStatement insertDm;
  private final PreparedStatement selectDm;
  private final PreparedStatement insertMessage;
  private final PreparedStatement selectBefore;
  private final PreparedStatement selectAfter;
  private final PreparedStatement insertDedup;
  private final PreparedStatement selectDedup;

  private final PreparedStatement markDelivered;
  private final PreparedStatement markRead;
  private final PreparedStatement inboxDelivered;
  private final PreparedStatement inboxRead;
  private final PreparedStatement stampState;
  private final PreparedStatement selectMarks;
  private final PreparedStatement markSentStmt;
  private final PreparedStatement selectSent;
  private final PreparedStatement selectOwn;
  private final PreparedStatement selectStates;
  private final PreparedStatement selectPrivacy;
  private final PreparedStatement setHidePresence;
  private final PreparedStatement setHideRead;
  private final PreparedStatement selectLastSeen;
  private final PreparedStatement upsertLastSeen;

  private final PreparedStatement selectMembersByJoin;
  private final PreparedStatement selectUser;
  private final PreparedStatement upsertUser;

  public ScyllaChatStore(CqlSession s, String ks) {
    this.s = s;
    insertConv = s.prepare("INSERT INTO " + ks + ".conversations (conv_id, kind, title, created_by, created_at, "
        + "last_seq, last_msg_at, member_count, is_public) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        + "IF NOT EXISTS");
    selectConv = s.prepare("SELECT conv_id, kind, title, created_by, last_seq, is_public, member_count FROM " + ks
        + ".conversations WHERE conv_id = ?");
    reserve = s.prepare("UPDATE " + ks + ".conversations SET last_seq = ? WHERE conv_id = ? IF last_seq = ?");
    selectLastSeq = s.prepare("SELECT last_seq FROM " + ks + ".conversations WHERE conv_id = ?");
    insertMember = s.prepare("INSERT INTO " + ks + ".members (conv_id, user_id, role, joined_at) VALUES (?, ?, ?, ?)");
    deleteMember = s.prepare("DELETE FROM " + ks + ".members WHERE conv_id = ? AND user_id = ?");
    selectMembers = s.prepare("SELECT user_id, role FROM " + ks + ".members WHERE conv_id = ?");
    selectMembersByJoin = s.prepare("SELECT user_id, role, joined_at FROM " + ks + ".members WHERE conv_id = ?");
    selectUser = s.prepare("SELECT user_id, status, updated_at, display_name FROM " + ks + ".users WHERE user_id = ?");
    upsertUser = s.prepare("INSERT INTO " + ks + ".users (user_id, status, updated_at, display_name) VALUES (?, ?, ?, ?)");
    selectMember = s.prepare("SELECT user_id, role FROM " + ks + ".members WHERE conv_id = ? AND user_id = ?");
    joinUserConv = s.prepare("UPDATE " + ks + ".user_convs SET kind = ?, left = false WHERE user_id = ? AND conv_id = ?");
    leaveUserConv = s.prepare("UPDATE " + ks + ".user_convs SET left = true WHERE user_id = ? AND conv_id = ?");
    // the cell timestamp is the conv_seq: concurrent or replayed touches keep the highest seq without an LWT
    touchUserConv = s.prepare("UPDATE " + ks + ".user_convs USING TIMESTAMP ? SET last_seq = ?, last_msg_at = ? "
        + "WHERE user_id = ? AND conv_id = ?");
    // CH3 §3.2: each mark cell's timestamp is its value, so a smaller mark never wins; state_at is wall time (µs)
    markDelivered = s.prepare("UPDATE " + ks + ".conv_marks USING TIMESTAMP ? SET delivered_seq = ? "
        + "WHERE conv_id = ? AND user_id = ?");
    markRead = s.prepare("UPDATE " + ks + ".conv_marks USING TIMESTAMP ? SET read_seq = ? WHERE conv_id = ? AND user_id = ?");
    inboxDelivered = s.prepare("UPDATE " + ks + ".user_convs USING TIMESTAMP ? SET delivered_seq = ? "
        + "WHERE user_id = ? AND conv_id = ?");
    inboxRead = s.prepare("UPDATE " + ks + ".user_convs USING TIMESTAMP ? SET read_seq = ? WHERE user_id = ? AND conv_id = ?");
    stampState = s.prepare("UPDATE " + ks + ".user_convs USING TIMESTAMP ? SET state_at = ? WHERE user_id = ? AND conv_id = ?");
    markSentStmt = s.prepare("UPDATE " + ks + ".conv_marks USING TIMESTAMP ? SET last_sent_seq = ? "
        + "WHERE conv_id = ? AND user_id = ?");
    selectSent = s.prepare("SELECT last_sent_seq FROM " + ks + ".conv_marks WHERE conv_id = ? AND user_id = ?");
    selectMarks = s.prepare("SELECT user_id, delivered_seq, read_seq FROM " + ks + ".conv_marks WHERE conv_id = ?");
    selectOwn = s.prepare("SELECT delivered_seq, read_seq FROM " + ks + ".user_convs WHERE user_id = ? AND conv_id = ?");
    selectStates = s.prepare("SELECT conv_id, kind, last_seq, read_seq, delivered_seq, state_at, left FROM " + ks
        + ".user_convs WHERE user_id = ?");
    selectPrivacy = s.prepare("SELECT hide_presence, hide_read FROM " + ks + ".user_settings WHERE user_id = ?");
    setHidePresence = s.prepare("UPDATE " + ks + ".user_settings SET hide_presence = ?, updated_at = ? WHERE user_id = ?");
    setHideRead = s.prepare("UPDATE " + ks + ".user_settings SET hide_read = ?, updated_at = ? WHERE user_id = ?");
    selectLastSeen = s.prepare("SELECT last_seen_ms FROM " + ks + ".last_seen WHERE user_id = ?");
    upsertLastSeen = s.prepare("INSERT INTO " + ks + ".last_seen (user_id, last_seen_ms) VALUES (?, ?)");
    selectUserConvs = s.prepare("SELECT conv_id, kind, last_seq, read_seq, last_msg_at, left, delivered_seq FROM " + ks
        + ".user_convs WHERE user_id = ?");
    insertDm = s.prepare("INSERT INTO " + ks + ".dm_index (user_lo, user_hi, conv_id) VALUES (?, ?, ?) IF NOT EXISTS");
    selectDm = s.prepare("SELECT conv_id FROM " + ks + ".dm_index WHERE user_lo = ? AND user_hi = ?");
    insertMessage = s.prepare("INSERT INTO " + ks + ".messages (conv_id, bucket, conv_seq, msg_id, sender_id, "
        + "client_msg_id, text, reply_to, mentions, sent_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    String cols = "conv_id, conv_seq, msg_id, sender_id, client_msg_id, text, reply_to, mentions, sent_at";
    selectBefore = s.prepare("SELECT " + cols + " FROM " + ks + ".messages WHERE conv_id = ? AND bucket = ? "
        + "AND conv_seq < ? ORDER BY conv_seq DESC LIMIT ?");
    selectAfter = s.prepare("SELECT " + cols + " FROM " + ks + ".messages WHERE conv_id = ? AND bucket = ? "
        + "AND conv_seq > ? ORDER BY conv_seq ASC LIMIT ?");
    insertDedup = s.prepare("INSERT INTO " + ks + ".send_dedup (conv_id, sender_id, client_msg_id, msg_id, conv_seq, "
        + "sent_at) VALUES (?, ?, ?, ?, ?, ?)");
    selectDedup = s.prepare("SELECT msg_id, conv_seq, sent_at FROM " + ks + ".send_dedup WHERE conv_id = ? "
        + "AND sender_id = ? AND client_msg_id = ?");
  }

  private ResultSet exec(Statement<?> st) {
    try {
      return s.execute(st);
    } catch (DriverException e) {
      throw new StoreUnavailableException("scylla: " + e.getMessage(), e);
    }
  }

  // --- conversations ----------------------------------------------------------------------------------------------

  @Override
  public Optional<Conversation> get(String id) {
    Row r = exec(selectConv.bind(id)).one();
    if (r == null) {
      return Optional.empty();
    }
    return Optional.of(new Conversation(r.getString("conv_id"), ConversationKind.forNumber(r.getInt("kind")),
        r.getString("title"), r.getString("created_by"), r.getLong("last_seq"), r.getBoolean("is_public"),
        r.getInt("member_count")));
  }

  @Override
  /** {@code IF NOT EXISTS}: creating an existing conversation again never resets its {@code last_seq} fence. */
  public void create(Conversation c, List<Member> members) {
    long now = System.currentTimeMillis();
    exec(insertConv.bind(c.id(), c.kind().getNumber(), c.title(), c.createdBy(), now, 0L, now, members.size(),
        c.isPublic()));
    for (Member m : members) {
      add(c.id(), c.kind(), m, now);
    }
  }

  @Override
  public boolean reserveSeq(String convId, long known, int n) {
    return exec(reserve.bind(known + n, convId, known)).wasApplied();
  }

  @Override
  public long lastSeq(String convId) {
    Row r = exec(selectLastSeq.bind(convId)).one();
    return r == null ? 0 : r.getLong(0);
  }

  @Override
  public Optional<String> dm(String userA, String userB) {
    String lo = userA.compareTo(userB) <= 0 ? userA : userB;
    String hi = lo.equals(userA) ? userB : userA;
    Row r = exec(selectDm.bind(lo, hi)).one();
    return r == null ? Optional.empty() : Optional.of(r.getString(0));
  }

  @Override
  public String createDm(String userA, String userB, String newConvId) {
    String lo = userA.compareTo(userB) <= 0 ? userA : userB;
    String hi = lo.equals(userA) ? userB : userA;
    ResultSet rs = exec(insertDm.bind(lo, hi, newConvId));
    if (rs.wasApplied()) {
      return newConvId;
    }
    return rs.one().getString("conv_id");   // an LWT that was not applied returns the existing row
  }

  // --- members ----------------------------------------------------------------------------------------------------

  @Override
  public List<Member> members(String convId) {
    List<Member> out = new ArrayList<>();
    for (Row r : exec(selectMembers.bind(convId))) {
      out.add(new Member(r.getString(0), Role.valueOf(r.getString(1))));
    }
    return out;
  }

  /** Earliest joined first (account spec §8 owner succession); ties by user id. */
  @Override
  public List<Member> membersByJoin(String convId) {
    record Joined(Member m, long at) {}
    List<Joined> all = new ArrayList<>();
    for (Row r : exec(selectMembersByJoin.bind(convId))) {
      all.add(new Joined(new Member(r.getString(0), Role.valueOf(r.getString(1))), r.getLong(2)));
    }
    all.sort(Comparator.comparingLong(Joined::at).thenComparing(j -> j.m().userId()));
    return all.stream().map(Joined::m).toList();
  }

  // --- users (account spec §8) --------------------------------------------------------------------------------------

  @Override
  public Optional<ChatStores.UserRecord> user(String userId) {
    Row r = exec(selectUser.bind(userId)).one();
    return r == null ? Optional.empty()
        : Optional.of(new ChatStores.UserRecord(r.getString(0), r.getString(1), r.getString(3), r.getLong(2)));
  }

  @Override
  public void put(String userId, String status, String displayName, long updatedAtMs) {
    exec(upsertUser.bind(userId, status, updatedAtMs, displayName));
  }

  @Override
  public Optional<Member> member(String convId, String userId) {
    Row r = exec(selectMember.bind(convId, userId)).one();
    return r == null ? Optional.empty() : Optional.of(new Member(r.getString(0), Role.valueOf(r.getString(1))));
  }

  @Override
  public void add(String convId, Member m) {
    ConversationKind kind = get(convId).map(Conversation::kind).orElse(ConversationKind.CONVERSATION_KIND_UNSPECIFIED);
    add(convId, kind, m, System.currentTimeMillis());
  }

  private void add(String convId, ConversationKind kind, Member m, long now) {
    exec(insertMember.bind(convId, m.userId(), m.role().name(), now));
    exec(joinUserConv.bind(kind.getNumber(), m.userId(), convId));
  }

  /** The user_convs row is flagged, not deleted: see the schema note on {@code left}. */
  @Override
  public void remove(String convId, String userId) {
    exec(deleteMember.bind(convId, userId));
    exec(leaveUserConv.bind(userId, convId));
  }

  /** Newest message first; {@code cursor} is {@code <lastMsgAtMs>:<convId>} of the previous page's last entry. */
  @Override
  public List<UserConv> userConvs(String userId, String cursor, int limit) {
    List<UserConv> all = new ArrayList<>();
    for (Row r : exec(selectUserConvs.bind(userId))) {
      if (r.getBoolean("left")) {
        continue;
      }
      all.add(new UserConv(r.getString("conv_id"), ConversationKind.forNumber(r.getInt("kind")), r.getLong("last_seq"),
          r.getLong("read_seq"), r.getLong("last_msg_at"), r.getLong("delivered_seq")));
    }
    Comparator<UserConv> order = Comparator.comparingLong(UserConv::lastMsgAtMs).reversed()
        .thenComparing(UserConv::convId);
    all.sort(order);
    int from = 0;
    if (cursor != null && !cursor.isEmpty()) {
      int colon = cursor.indexOf(':');
      UserConv after = new UserConv(cursor.substring(colon + 1), null, 0, 0, Long.parseLong(cursor.substring(0, colon)));
      while (from < all.size() && order.compare(all.get(from), after) <= 0) {
        from++;
      }
    }
    return List.copyOf(all.subList(from, Math.min(all.size(), from + limit)));
  }

  @Override
  public void touchUserConv(String userId, String convId, long seq, long atMs) {
    exec(com.datastax.oss.driver.api.core.cql.BatchStatement.newInstance(
        com.datastax.oss.driver.api.core.cql.DefaultBatchType.UNLOGGED,
        touchUserConv.bind(seq, seq, atMs, userId, convId),
        stamp(userId, convId, System.currentTimeMillis())));   // final review I5: state_at = when written
  }

  // --- CH3 marks, state, privacy, last seen -----------------------------------------------------------------------

  private com.datastax.oss.driver.api.core.cql.BoundStatement stamp(String userId, String convId, long atMs) {
    return stampState.bind(atMs * 1000, atMs, userId, convId);
  }

  @Override
  public void advance(String convId, String userId, long deliveredSeq, long readSeq, long atMs) {
    advance(convId, userId, deliveredSeq, readSeq, 0, atMs);
  }

  /** Review of PR #6: the sender's three marks go in the same conv_marks batch, one round trip per message. */
  @Override
  public void sent(String convId, String userId, long seq, long atMs) {
    advance(convId, userId, seq, seq, seq, atMs);
  }

  private void advance(String convId, String userId, long deliveredSeq, long readSeq, long sentSeq, long atMs) {
    var conv = com.datastax.oss.driver.api.core.cql.BatchStatement.builder(
        com.datastax.oss.driver.api.core.cql.DefaultBatchType.UNLOGGED);
    var inbox = com.datastax.oss.driver.api.core.cql.BatchStatement.builder(
        com.datastax.oss.driver.api.core.cql.DefaultBatchType.UNLOGGED);
    if (deliveredSeq > 0) {
      conv.addStatement(markDelivered.bind(deliveredSeq, deliveredSeq, convId, userId));
      inbox.addStatement(inboxDelivered.bind(deliveredSeq, deliveredSeq, userId, convId));
    }
    if (readSeq > 0) {
      conv.addStatement(markRead.bind(readSeq, readSeq, convId, userId));
      inbox.addStatement(inboxRead.bind(readSeq, readSeq, userId, convId));
    }
    if (sentSeq > 0) {
      conv.addStatement(markSentStmt.bind(sentSeq, sentSeq, convId, userId));
    }
    if (deliveredSeq <= 0 && readSeq <= 0) {
      return;
    }
    inbox.addStatement(stamp(userId, convId, atMs));
    exec(conv.build());
    exec(inbox.build());
  }

  @Override
  public long sentSeq(String convId, String userId) {
    Row r = exec(selectSent.bind(convId, userId)).one();
    return r == null ? 0 : r.getLong(0);
  }

  @Override
  public void touchState(String userId, String convId, long atMs) {
    exec(stamp(userId, convId, atMs));
  }

  @Override
  public List<ChatStores.MemberMarks> marks(String convId) {
    List<ChatStores.MemberMarks> out = new ArrayList<>();
    for (Row r : exec(selectMarks.bind(convId))) {
      out.add(new ChatStores.MemberMarks(r.getString(0), r.getLong(1), r.getLong(2)));
    }
    return out;
  }

  @Override
  public ChatStores.Marks own(String userId, String convId) {
    Row r = exec(selectOwn.bind(userId, convId)).one();
    return r == null ? ChatStores.Marks.NONE : new ChatStores.Marks(r.getLong(0), r.getLong(1));
  }

  @Override
  public List<ChatStores.UserConvState> changedSince(String userId, long sinceMs, String cursor, int limit) {
    List<ChatStores.UserConvState> all = new ArrayList<>();
    for (Row r : exec(selectStates.bind(userId))) {
      if (r.getBoolean("left") || r.getLong("state_at") <= sinceMs) {
        continue;
      }
      all.add(new ChatStores.UserConvState(r.getString("conv_id"), ConversationKind.forNumber(r.getInt("kind")),
          r.getLong("last_seq"), r.getLong("read_seq"), r.getLong("delivered_seq"), r.getLong("state_at")));
    }
    Comparator<ChatStores.UserConvState> order = Comparator.comparingLong(ChatStores.UserConvState::stateAtMs)
        .reversed().thenComparing(ChatStores.UserConvState::convId);
    all.sort(order);
    int from = 0;
    if (cursor != null && !cursor.isEmpty()) {
      int colon = cursor.indexOf(':');
      var after = new ChatStores.UserConvState(cursor.substring(colon + 1), null, 0, 0, 0,
          Long.parseLong(cursor.substring(0, colon)));
      while (from < all.size() && order.compare(all.get(from), after) <= 0) {
        from++;
      }
    }
    return List.copyOf(all.subList(from, Math.min(all.size(), from + limit)));
  }

  @Override
  public ChatStores.Privacy privacy(String userId) {
    Row r = exec(selectPrivacy.bind(userId)).one();
    return r == null ? ChatStores.Privacy.OPEN : new ChatStores.Privacy(r.getBoolean(0), r.getBoolean(1));
  }

  @Override
  public void updatePrivacy(String userId, Boolean hidePresence, Boolean hideRead, long atMs) {
    var b = com.datastax.oss.driver.api.core.cql.BatchStatement.builder(
        com.datastax.oss.driver.api.core.cql.DefaultBatchType.UNLOGGED);
    if (hidePresence != null) {
      b.addStatement(setHidePresence.bind(hidePresence, atMs, userId));
    }
    if (hideRead != null) {
      b.addStatement(setHideRead.bind(hideRead, atMs, userId));
    }
    if (hidePresence != null || hideRead != null) {
      exec(b.build());
    }
  }

  @Override
  public java.util.OptionalLong lastSeen(String userId) {
    Row r = exec(selectLastSeen.bind(userId)).one();
    return r == null || r.isNull(0) ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(r.getLong(0));
  }

  @Override
  public void putLastSeen(String userId, long atMs) {
    exec(upsertLastSeen.bind(userId, atMs));
  }

  // --- messages ---------------------------------------------------------------------------------------------------

  @Override
  public void write(StoredMessage m) {
    exec(insertMessage.bind(m.convId(), m.convSeq() / BUCKET, m.convSeq(), m.msgId(), m.senderId(), m.clientMsgId(),
        m.text(), m.replyTo(), m.mentions(), m.sentAtMs()));
  }

  @Override
  public List<StoredMessage> before(String convId, long beforeSeq, int limit) {
    List<StoredMessage> out = new ArrayList<>();
    if (beforeSeq <= 0) {
      return out;
    }
    long last = Math.max(0, beforeSeq - limit) / BUCKET;
    for (long b = (beforeSeq - 1) / BUCKET; b >= last && out.size() < limit; b--) {
      for (Row r : exec(selectBefore.bind(convId, b, beforeSeq, limit - out.size()))) {
        out.add(message(r));
      }
    }
    return out;
  }

  @Override
  public List<StoredMessage> after(String convId, long afterSeq, int limit) {
    List<StoredMessage> out = new ArrayList<>();
    long last = (afterSeq + limit) / BUCKET;
    for (long b = (afterSeq + 1) / BUCKET; b <= last && out.size() < limit; b++) {
      for (Row r : exec(selectAfter.bind(convId, b, afterSeq, limit - out.size()))) {
        out.add(message(r));
      }
    }
    return out;
  }

  private static StoredMessage message(Row r) {
    return new StoredMessage(r.getString("conv_id"), r.getLong("conv_seq"), r.getString("msg_id"),
        r.getString("sender_id"), r.getString("client_msg_id"), r.getString("text"), r.getString("reply_to"),
        r.getList("mentions", String.class), r.getLong("sent_at"));
  }

  // --- dedup ------------------------------------------------------------------------------------------------------

  @Override
  public Optional<StoredMessage> find(String convId, String senderId, String clientMsgId) {
    Row r = exec(selectDedup.bind(convId, senderId, clientMsgId)).one();
    if (r == null) {
      return Optional.empty();
    }
    return Optional.of(new StoredMessage(convId, r.getLong("conv_seq"), r.getString("msg_id"), senderId, clientMsgId,
        "", "", List.of(), r.getLong("sent_at")));
  }

  @Override
  public void put(StoredMessage m) {
    exec(insertDedup.bind(m.convId(), m.senderId(), m.clientMsgId(), m.msgId(), m.convSeq(), m.sentAtMs()));
  }
}
