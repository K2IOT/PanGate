package com.pangate.chat.core;

import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PresenceUpdate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * CH3 spec §6.3: a device says whom it shows on screen, each with a conversation both share. An entry is kept only
 * when both are members of that conversation and it is not a channel, and (owner decision 2026-11-17) the watched
 * user sent at least one message in it, DM or group alike. Others are dropped without a word (nothing tells whether
 * such a user exists). The device's list is replaced, kept for {@code ttl}, and answered at once with
 * a snapshot to that device. The watch of a user who hides presence is still stored: typing uses it.
 */
public final class PresenceWatch {

  private final ChatStores.ConversationStore conversations;
  private final MemberLookup members;
  private final ChatStores.MarkStore marks;
  private final Watchers watchers;
  private final PresenceState presence;
  private final ChatStores.LastSeenStore durable;
  private final PrivacyCache privacy;
  private final Pusher pusher;
  private final LongSupplier wallMs;
  private final long ttlMs;
  private final int max;

  public PresenceWatch(ChatStores.ConversationStore conversations, MemberLookup members,
      ChatStores.MarkStore marks, Watchers watchers, PresenceState presence, ChatStores.LastSeenStore durable,
      PrivacyCache privacy, Pusher pusher,
      LongSupplier wallMs, Duration ttl, int max) {
    this.conversations = conversations;
    this.members = members;
    this.marks = marks;
    this.watchers = watchers;
    this.presence = presence;
    this.durable = durable;
    this.privacy = privacy;
    this.pusher = pusher;
    this.wallMs = wallMs;
    this.ttlMs = ttl.toMillis();
    this.max = max;
  }

  /** Returns how many entries were kept. */
  public int watch(String viewer, String device, com.pangate.contracts.v1.PresenceWatch msg) {
    List<com.pangate.contracts.v1.PresenceWatch.Entry> valid = new ArrayList<>();
    Set<String> open = new LinkedHashSet<>();
    int seen = 0;
    Map<String, Optional<Set<String>>> looked = new HashMap<>();
    for (com.pangate.contracts.v1.PresenceWatch.Entry e : msg.getEntriesList()) {
      if (seen++ >= max) {
        break;
      }
      // Codex review of PR #6: the conversation is open for typing even when no entry in it may show presence
      if (!open.contains(e.getConversationId()) && openable(looked, viewer, e.getConversationId()).isPresent()) {
        open.add(e.getConversationId());
      }
      if (open.contains(e.getConversationId()) && allowed(looked, viewer, e.getUserId(), e.getConversationId())) {
        valid.add(e);
      }
    }
    long now = wallMs.getAsLong();
    watchers.replace(viewer, device, valid, open, now + ttlMs);
    Set<String> users = new LinkedHashSet<>();
    valid.forEach(e -> users.add(e.getUserId()));
    if (!users.isEmpty()) {
      PresenceUpdate.Builder u = PresenceUpdate.newBuilder();
      users.forEach(user -> u.addStates(stateFor(user, viewer, now)));
      pusher.send(StatePushes.ephemeral(viewer, device, ChatTopics.TYPE_PRESENCE, u.build().toByteString(),
          StatePushes.PRESENCE_TTL_SEC), Pusher.Priority.DROPPABLE);
    }
    return valid.size();
  }

  /** {@code GET /v1/presence}: the states of {@code ids} that share {@code convId} with the viewer. */
  public List<PresenceUpdate.State> states(String viewer, String convId, List<String> ids) {
    long now = wallMs.getAsLong();
    List<PresenceUpdate.State> out = new ArrayList<>();
    Map<String, Optional<Set<String>>> looked = new HashMap<>();
    for (String id : new LinkedHashSet<>(ids)) {
      if (allowed(looked, viewer, id, convId)) {
        out.add(stateFor(id, viewer, now));
      }
    }
    return out;
  }

  /**
   * The members of a DM or group (not a channel) the viewer is a member of, else empty. Perf 3a (spec §3.1): looked up
   * once per conversation per command in {@code looked}, however many entries point at it.
   */
  private Optional<Set<String>> openable(Map<String, Optional<Set<String>>> looked, String viewer, String convId) {
    return looked.computeIfAbsent(convId, id -> {
      if (!Ids.isUlid(id)) {
        return Optional.empty();
      }
      Optional<ChatStores.Conversation> c = conversations.get(id);
      if (c.isEmpty() || c.get().kind() == ConversationKind.CONVERSATION_KIND_CHANNEL) {
        return Optional.empty();
      }
      Set<String> ids = new HashSet<>();
      members.of(id).forEach(m -> ids.add(m.userId()));
      return ids.contains(viewer) ? Optional.of(ids) : Optional.empty();
    });
  }

  private boolean allowed(Map<String, Optional<Set<String>>> looked, String viewer, String user, String convId) {
    if (user.isEmpty() || user.equals(viewer)) {
      return false;
    }
    Optional<Set<String>> ms = openable(looked, viewer, convId);
    if (ms.isEmpty() || !ms.get().contains(user)) {
      return false;
    }
    // owner decision 2026-11-17: a DM or group proves a relationship only once the watched user sent a message in it
    // (anyone can open an empty DM, and a second account can create a group)
    return marks.sentSeq(convId, user) > 0;
  }

  PresenceUpdate.State stateFor(String user, String viewer, long now) {
    boolean online = presence.online(user);
    OptionalLong last = online ? OptionalLong.empty() : lastSeen(user);
    return PresenceView.state(user, online, last, privacy.of(user), privacy.of(viewer), now);
  }

  private OptionalLong lastSeen(String user) {
    OptionalLong v = presence.lastSeen(user);
    return v.isPresent() ? v : durable.lastSeen(user);
  }
}
