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
public final class ScyllaChatStore implements ChatStores.ConversationStore, ChatStores.MemberStore,
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
    selectMember = s.prepare("SELECT user_id, role FROM " + ks + ".members WHERE conv_id = ? AND user_id = ?");
    joinUserConv = s.prepare("UPDATE " + ks + ".user_convs SET kind = ?, left = false WHERE user_id = ? AND conv_id = ?");
    leaveUserConv = s.prepare("UPDATE " + ks + ".user_convs SET left = true WHERE user_id = ? AND conv_id = ?");
    // the cell timestamp is the conv_seq: concurrent or replayed touches keep the highest seq without an LWT
    touchUserConv = s.prepare("UPDATE " + ks + ".user_convs USING TIMESTAMP ? SET last_seq = ?, last_msg_at = ? "
        + "WHERE user_id = ? AND conv_id = ?");
    selectUserConvs = s.prepare("SELECT conv_id, kind, last_seq, read_seq, last_msg_at, left FROM " + ks
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
          r.getLong("read_seq"), r.getLong("last_msg_at")));
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
    exec(touchUserConv.bind(seq, seq, atMs, userId, convId));
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
