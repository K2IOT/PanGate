package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.MemberMarks;
import com.pangate.chat.core.ChatStores.Privacy;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.ChatStores.UserConv;
import com.pangate.chat.core.ChatStores.UserConvState;
import com.pangate.contracts.v1.ConversationKind;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * CH3 spec §3.4, §4, §7.2, §8: unread counts, the DM peer's marks, {@code /v1/state} pages and receipt lookups,
 * every one filtered by the reciprocal privacy rules.
 */
public final class StateService {

  public static final int MAX_UNREAD = 999;
  public static final int STATE_PAGE = 200;
  /** Final review I5: "now" is this much early, so writes in flight (or on a slower pod clock) are not missed. */
  public static final long SINCE_MARGIN_MS = 30_000;
  static final long COUNT_TTL_MS = 5_000;

  public record StatePage(List<Map<String, Object>> conversations, String cursor, long now) {}

  private record Count(Integer value, long until) {}

  private final ChatStores.ConversationStore conversations;
  private final MemberLookup members;
  private final ChatStores.MarkStore marks;
  private final ChatStores.MemberStore inbox;
  private final ChatStores.MessageStore messages;
  private final PrivacyCache privacy;
  private final int listedMax;
  private final LongSupplier monoMs;
  private final LongSupplier wallMs;
  private final TokenBucketLimiter receiptLookups;
  private final Map<String, Count> counts = new ConcurrentHashMap<>();

  public StateService(ChatStores.ConversationStore conversations, MemberLookup members, ChatStores.MarkStore marks,
      ChatStores.MemberStore inbox, ChatStores.MessageStore messages, PrivacyCache privacy, int listedMax,
      LongSupplier monoMs, LongSupplier wallMs) {
    this.conversations = conversations;
    this.members = members;
    this.marks = marks;
    this.inbox = inbox;
    this.messages = messages;
    this.privacy = privacy;
    this.listedMax = listedMax;
    this.monoMs = monoMs;
    this.wallMs = wallMs;
    this.receiptLookups = new TokenBucketLimiter(1, 1, () -> monoMs.getAsLong() * 1_000_000);
  }

  public static int unread(long lastSeq, long readSeq) {
    return (int) Math.min(MAX_UNREAD, Math.max(0, lastSeq - readSeq));
  }

  public static String kindName(ConversationKind k) {
    return switch (k == null ? ConversationKind.CONVERSATION_KIND_UNSPECIFIED : k) {
      case CONVERSATION_KIND_DIRECT -> "direct";
      case CONVERSATION_KIND_GROUP -> "group";
      case CONVERSATION_KIND_CHANNEL -> "channel";
      default -> "unknown";
    };
  }

  public static UserConvState of(UserConv u) {
    return new UserConvState(u.convId(), u.kind(), u.lastSeq(), u.readSeq(), u.deliveredSeq(), 0);
  }

  /** {@code id, kind, lastSeq, readSeq, deliveredSeq, unread}, and {@code peer} for a DM. */
  public Map<String, Object> conversationView(String viewer, UserConvState s) {
    long lastSeq = s.kind() == ConversationKind.CONVERSATION_KIND_CHANNEL ? conversations.lastSeq(s.convId())
        : s.lastSeq();
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("id", s.convId());
    m.put("kind", kindName(s.kind()));
    m.put("lastSeq", lastSeq);
    m.put("readSeq", s.readSeq());
    m.put("deliveredSeq", s.deliveredSeq());
    m.put("unread", unread(lastSeq, s.readSeq()));
    if (s.kind() == ConversationKind.CONVERSATION_KIND_DIRECT) {
      m.put("peer", peer(viewer, s.convId()));
    }
    return m;
  }

  private Map<String, Object> peer(String viewer, String convId) {
    String other = members.of(convId).stream().map(Member::userId).filter(u -> !u.equals(viewer)).findFirst()
        .orElse("");
    MemberMarks mm = marksOf(convId).getOrDefault(other, new MemberMarks(other, 0, 0));
    boolean showRead = PrivacyRules.mayShowRead(privacy.of(other), privacy.of(viewer));
    Map<String, Object> p = new LinkedHashMap<>();
    p.put("userId", other);
    p.put("deliveredSeq", mm.deliveredSeq());
    p.put("readSeq", showRead ? mm.readSeq() : 0L);
    return p;
  }

  private Map<String, MemberMarks> marksOf(String convId) {
    Map<String, MemberMarks> by = new HashMap<>();
    for (MemberMarks mm : marks.marks(convId)) {
      by.put(mm.userId(), mm);
    }
    return by;
  }

  /** CH3 spec §7.2. No {@code since}: every conversation, like {@code GET /v1/conversations}. */
  public StatePage state(String viewer, OptionalLong sinceMs, String cursor) {
    long now = wallMs.getAsLong() - SINCE_MARGIN_MS;   // overlap is harmless: entries are full state
    List<Map<String, Object>> out = new ArrayList<>();
    String next = null;
    if (sinceMs.isEmpty()) {
      List<UserConv> page = inbox.userConvs(viewer, cursor, STATE_PAGE);
      page.forEach(u -> out.add(conversationView(viewer, of(u))));
      if (page.size() == STATE_PAGE) {
        next = page.getLast().lastMsgAtMs() + ":" + page.getLast().convId();
      }
    } else {
      List<UserConvState> page = marks.changedSince(viewer, sinceMs.getAsLong(), cursor, STATE_PAGE);
      page.forEach(u -> out.add(conversationView(viewer, u)));
      if (page.size() == STATE_PAGE) {
        next = page.getLast().stateAtMs() + ":" + page.getLast().convId();
      }
    }
    return new StatePage(out, next, now);
  }

  /** CH3 spec §3.4: DM and groups up to {@code listedMax}: marks; larger groups: a count; channels: refused. */
  public Map<String, Object> receipts(String viewer, String convId, long seq) {
    ChatStores.Conversation c = conversations.get(convId).orElseThrow(ChatRefusal::notFound);
    if (c.kind() == ConversationKind.CONVERSATION_KIND_CHANNEL) {
      throw new ChatRefusal(ChatRefusal.NOT_SUPPORTED, "channels have no receipts");
    }
    List<Member> current = members.of(convId);
    if (current.stream().noneMatch(m -> m.userId().equals(viewer))) {
      throw ChatRefusal.forbidden("not a member");
    }
    if (!receiptLookups.tryAcquire(viewer + "|" + convId)) {
      throw new ChatRefusal(ChatRefusal.RATE_LIMITED, "one receipts lookup per second");
    }
    Privacy pv = privacy.of(viewer);
    Map<String, Object> out = new LinkedHashMap<>();
    if (c.kind() == ConversationKind.CONVERSATION_KIND_DIRECT || current.size() <= listedMax) {
      Map<String, MemberMarks> by = marksOf(convId);
      List<Map<String, Object>> list = new ArrayList<>();
      for (Member m : current) {
        if (m.userId().equals(viewer)) {
          continue;
        }
        MemberMarks mm = by.getOrDefault(m.userId(), new MemberMarks(m.userId(), 0, 0));
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("userId", m.userId());
        e.put("deliveredSeq", mm.deliveredSeq());
        e.put("readSeq", PrivacyRules.mayShowRead(privacy.of(m.userId()), pv) ? mm.readSeq() : 0L);
        list.add(e);
      }
      out.put("marks", list);
      return out;
    }
    out.put("readCount", pv.hideRead() ? null : readCount(convId, seq, current));
    return out;
  }

  private Integer readCount(String convId, long seq, List<Member> current) {
    String key = convId + "|" + seq;
    long now = monoMs.getAsLong();
    Count cached = counts.get(key);
    if (cached != null && now < cached.until()) {
      return cached.value();
    }
    String sender = messages.after(convId, seq - 1, 1).stream().filter(m -> m.convSeq() == seq)
        .map(StoredMessage::senderId).findFirst().orElse("");
    Map<String, MemberMarks> by = marksOf(convId);
    int n = 0;
    for (Member m : current) {
      MemberMarks mm = by.get(m.userId());
      if (!m.userId().equals(sender) && mm != null && mm.readSeq() >= seq && !privacy.of(m.userId()).hideRead()) {
        n++;
      }
    }
    if (counts.size() > 100_000) {
      counts.clear();
    }
    counts.put(key, new Count(n, now + COUNT_TTL_MS));
    return n;
  }
}
