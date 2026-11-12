package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.contracts.v1.ConversationKind;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * {@code POST /v1/sync} (chat spec §7): per conversation the messages after the client's cursor, at most
 * {@code perConv} (default 50, at most 100); a larger gap answers {@code tooMany} so the client loads the latest page.
 * A cursor at or past the conversation's {@code last_seq} gets nothing (no error). Conversations the user may not read
 * are left out.
 */
public final class SyncService {

  public static final int MAX_CONVERSATIONS = 100;
  public static final int DEFAULT_PER_CONV = 50;
  public static final int MAX_PER_CONV = 100;

  public record SyncCursor(String convId, long lastSeq) {}

  public record SyncResult(String convId, List<StoredMessage> messages, boolean tooMany, long lastSeq) {}

  private final ChatStores.ConversationStore conversations;
  private final ChatStores.MemberStore members;
  private final ChatStores.MessageStore messages;

  public SyncService(ChatStores.ConversationStore conversations, ChatStores.MemberStore members,
      ChatStores.MessageStore messages) {
    this.conversations = conversations;
    this.members = members;
    this.messages = messages;
  }

  public List<SyncResult> sync(String userId, List<SyncCursor> in, int perConv) {
    if (in.size() > MAX_CONVERSATIONS) {
      throw ChatRefusal.invalid("at most " + MAX_CONVERSATIONS + " conversations per sync");
    }
    if (perConv < 0 || perConv > MAX_PER_CONV) {
      throw ChatRefusal.invalid("limitPerConversation is 1.." + MAX_PER_CONV);
    }
    int limit = perConv == 0 ? DEFAULT_PER_CONV : perConv;
    for (SyncCursor c : in) {
      if (c.lastSeq() < 0 || c.convId() == null) {
        throw ChatRefusal.invalid("lastSeq must be >= 0");
      }
    }
    List<SyncResult> out = new ArrayList<>();
    for (SyncCursor c : in) {
      Optional<Conversation> conv = conversations.get(c.convId());
      if (conv.isEmpty() || !readable(userId, conv.get())) {
        continue;
      }
      long last = conv.get().lastSeq();
      if (c.lastSeq() >= last) {
        out.add(new SyncResult(c.convId(), List.of(), false, last));
      } else if (last - c.lastSeq() > limit) {
        out.add(new SyncResult(c.convId(), List.of(), true, last));
      } else {
        out.add(new SyncResult(c.convId(), messages.after(c.convId(), c.lastSeq(), limit), false, last));
      }
    }
    return out;
  }

  private boolean readable(String userId, Conversation c) {
    return (c.kind() == ConversationKind.CONVERSATION_KIND_CHANNEL && c.isPublic())
        || members.member(c.id(), userId).isPresent();
  }
}
