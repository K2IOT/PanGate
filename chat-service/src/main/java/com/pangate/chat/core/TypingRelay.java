package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Member;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.TypingUpdate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * CH3 spec §5: "typing" goes straight to the other members (no Kafka; losing one does not matter), as an EPHEMERAL
 * push the client clears after {@link #TTL_MS}. At most one per 2 s per (user, conversation) and one fan-out per
 * second per conversation on this pod; none in channels. Groups above {@code listedMax} only reach devices that have
 * the conversation open. Returns {@code relayed} or why it was dropped.
 */
public final class TypingRelay {

  public static final int TTL_MS = 6_000;
  static final long PER_USER_MS = 2_000;
  static final long PER_CONV_MS = 1_000;
  static final int MAX_KEYS = 200_000;

  private final ChatStores.ConversationStore conversations;
  private final MemberLookup members;
  private final Pusher pusher;
  private final OpenConversations open;
  private final LongSupplier monoMs;
  private final int listedMax;
  private final Map<String, Long> lastByUser = new ConcurrentHashMap<>();
  private final Map<String, Long> lastByConv = new ConcurrentHashMap<>();

  public TypingRelay(ChatStores.ConversationStore conversations, MemberLookup members, Pusher pusher,
      OpenConversations open, LongSupplier monoMs, int listedMax) {
    this.conversations = conversations;
    this.members = members;
    this.pusher = pusher;
    this.open = open;
    this.monoMs = monoMs;
    this.listedMax = listedMax;
  }

  public String typing(String userId, String convId) {
    if (!Ids.isUlid(convId)) {
      return "not_member";
    }
    List<Member> current = members.of(convId);
    if (current.stream().noneMatch(m -> m.userId().equals(userId))) {
      return "not_member";
    }
    Optional<ChatStores.Conversation> c = conversations.get(convId);
    if (c.isEmpty()) {
      return "not_member";
    }
    if (c.get().kind() == ConversationKind.CONVERSATION_KIND_CHANNEL) {
      return "channel";
    }
    long now = monoMs.getAsLong();
    Long prev = lastByUser.get(userId + "|" + convId);
    if (prev != null && now - prev < PER_USER_MS) {
      return "too_often";
    }
    boolean large = current.size() > listedMax;
    Long conv = lastByConv.get(convId);
    if (large && conv != null && now - conv < PER_CONV_MS) {   // final review I9: large groups only (spec §5)
      return "conversation_busy";
    }
    if (lastByUser.size() > MAX_KEYS || lastByConv.size() > MAX_KEYS) {
      lastByUser.clear();
      lastByConv.clear();
    }
    lastByUser.put(userId + "|" + convId, now);
    if (large) {
      lastByConv.put(convId, now);
    }
    var payload = TypingUpdate.newBuilder().setConversationId(convId).setUserId(userId).setTtlMs(TTL_MS).build()
        .toByteString();
    if (!large) {
      for (Member m : current) {
        if (!m.userId().equals(userId)) {
          send(StatePushes.ephemeral(m.userId(), "", ChatTopics.TYPE_TYPING, payload, StatePushes.TYPING_TTL_SEC));
        }
      }
    } else {
      java.util.Set<String> devices;
      try {
        devices = open.devicesWithOpen(convId);
      } catch (StoreUnavailableException e) {
        return "unavailable";
      }
      for (String d : devices) {
        int slash = d.indexOf('/');
        String u = d.substring(0, slash);
        if (!u.equals(userId) && current.stream().anyMatch(m -> m.userId().equals(u))) {
          send(StatePushes.ephemeral(u, d.substring(slash + 1), ChatTopics.TYPE_TYPING, payload,
              StatePushes.TYPING_TTL_SEC));
        }
      }
    }
    return "relayed";
  }

  private void send(PushRequest r) {
    try {
      pusher.send(r);   // fire and forget: a lost typing push does not matter
    } catch (RuntimeException e) {
      // dropped
    }
  }
}
