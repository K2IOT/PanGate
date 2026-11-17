package com.pangate.chat.core;

import com.pangate.contracts.v1.ConversationKind;
import java.util.List;
import java.util.Optional;

/** Storage ports of the chat core (chat spec §5). Every method may throw {@link StoreUnavailableException}. */
public final class ChatStores {

  private ChatStores() {}

  public record Conversation(String id, ConversationKind kind, String title, String createdBy, long lastSeq,
      boolean isPublic, int memberCount) {}

  public record StoredMessage(String convId, long convSeq, String msgId, String senderId, String clientMsgId,
      String text, String replyTo, List<String> mentions, long sentAtMs) {}

  public enum Role { OWNER, ADMIN, MEMBER }

  public record Member(String userId, Role role) {}

  /** One row of a user's inbox; {@code deliveredSeq} / {@code readSeq} are the user's own marks (CH3 §3.2). */
  public record UserConv(String convId, ConversationKind kind, long lastSeq, long readSeq, long lastMsgAtMs,
      long deliveredSeq) {
    public UserConv(String convId, ConversationKind kind, long lastSeq, long readSeq, long lastMsgAtMs) {
      this(convId, kind, lastSeq, readSeq, lastMsgAtMs, 0);
    }
  }

  /** A user's delivered / read marks in one conversation (CH3 §3.1); 0 = none. */
  public record Marks(long deliveredSeq, long readSeq) {
    public static final Marks NONE = new Marks(0, 0);
  }

  public record MemberMarks(String userId, long deliveredSeq, long readSeq) {}

  /** An inbox row with its marks and the time its state last changed (CH3 §7.2). */
  public record UserConvState(String convId, ConversationKind kind, long lastSeq, long readSeq, long deliveredSeq,
      long stateAtMs) {}

  /** CH3 §8: what a user hides from others (and, reciprocally, does not see of others). */
  public record Privacy(boolean hidePresence, boolean hideRead) {
    public static final Privacy OPEN = new Privacy(false, false);
  }

  /**
   * Delivered / read marks (CH3 §3.2). Each mark column is only ever written with its own value as the cell
   * timestamp, so a smaller, late or replayed mark never moves a mark back. 0 = leave that column.
   */
  public interface MarkStore {
    /** Raises the user's marks in {@code conv_marks} and {@code user_convs}, and stamps the inbox row's state. */
    void advance(String convId, String userId, long deliveredSeq, long readSeq, long atMs);

    /** Stamps the inbox row's state time only (the other member of a DM saw a mark move). */
    void touchState(String userId, String convId, long atMs);

    /** Marks of everyone who ever marked this conversation; callers filter by the current members. */
    List<MemberMarks> marks(String convId);

    /** The user's own marks; {@link Marks#NONE} when none. */
    Marks own(String userId, String convId);

    /**
     * The user sent message {@code seq} here (fan-out): delivered and read up to it (§3.1), and the highest seq they
     * sent (the presence rule, §6.3), never back. One write.
     */
    void sent(String convId, String userId, long seq, long atMs);

    /** The highest seq this user sent in the conversation; 0 = never sent there. */
    long sentSeq(String convId, String userId);

    /** Inbox rows (not left) whose state changed after {@code sinceMs}, newest first; cursor {@code <stateAt>:<convId>}. */
    List<UserConvState> changedSince(String userId, long sinceMs, String cursor, int limit);
  }

  public interface SettingsStore {
    /** {@link Privacy#OPEN} when the user never set anything. */
    Privacy privacy(String userId);

    /**
     * Writes only the fields given (null = unchanged), so concurrent updates of different fields both stick (Codex
     * review of PR #6).
     */
    void updatePrivacy(String userId, Boolean hidePresence, Boolean hideRead, long atMs);

    default void putPrivacy(String userId, Privacy p, long atMs) {
      updatePrivacy(userId, p.hidePresence(), p.hideRead(), atMs);
    }
  }

  public interface LastSeenStore {
    java.util.OptionalLong lastSeen(String userId);

    void putLastSeen(String userId, long atMs);
  }

  public interface ConversationStore {
    Optional<Conversation> get(String id);

    /** Writes the conversation, its members and their user_convs rows. */
    void create(Conversation c, List<Member> members);

    /** LWT: {@code last_seq = known + n IF last_seq = known}; false when another writer moved it. */
    boolean reserveSeq(String convId, long known, int n);

    long lastSeq(String convId);

    Optional<String> dm(String userA, String userB);

    /** The DM of the pair: {@code newConvId} if this call created it, else the existing one. */
    String createDm(String userA, String userB, String newConvId);
  }

  public interface MemberStore {
    List<Member> members(String convId);

    Optional<Member> member(String convId, String userId);

    void add(String convId, Member m);

    void remove(String convId, String userId);

    /** Conversations of a user; {@code cursor} is the last conv id of the previous page, or null. */
    List<UserConv> userConvs(String userId, String cursor, int limit);

    /** Raises the user's last_seq / last_msg_at for the conversation; a lower seq never wins. */
    void touchUserConv(String userId, String convId, long seq, long atMs);

    /** Members, earliest joined first (owner succession, account spec §8). */
    default List<Member> membersByJoin(String convId) {
      return members(convId);
    }
  }

  /** {@code status}: {@code active} or {@code deleted} (account spec §8). */
  public record UserRecord(String userId, String status, long updatedAtMs) {}

  /** Users known from account-service's {@code account.users} (account spec §8). */
  public interface UserStore {
    Optional<UserRecord> user(String userId);

    /** Writes unconditionally (the caller already compared with what is stored). */
    void put(String userId, String status, long updatedAtMs);
  }

  public interface MessageStore {
    void write(StoredMessage m);

    /** Messages with conv_seq < beforeSeq, newest first. */
    List<StoredMessage> before(String convId, long beforeSeq, int limit);

    /** Messages with conv_seq > afterSeq, oldest first. */
    List<StoredMessage> after(String convId, long afterSeq, int limit);
  }

  public interface DedupStore {
    /** The message an earlier send with this key produced; only convId, convSeq, msgId, ids and sentAt are set. */
    Optional<StoredMessage> find(String convId, String senderId, String clientMsgId);

    void put(StoredMessage m);
  }

  /** Member lists in front of {@link MemberStore} (Redis, TTL 60 s); a miss or an outage reads as empty. */
  public interface MemberCache {
    Optional<List<Member>> cachedMembers(String convId);

    void putMembers(String convId, List<Member> members);

    /** False when the cached list may still be there (Redis did not confirm). */
    boolean evictMembers(String convId);
  }
}
