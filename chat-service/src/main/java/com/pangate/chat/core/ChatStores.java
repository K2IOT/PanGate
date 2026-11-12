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

  public record UserConv(String convId, ConversationKind kind, long lastSeq, long readSeq, long lastMsgAtMs) {}

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
