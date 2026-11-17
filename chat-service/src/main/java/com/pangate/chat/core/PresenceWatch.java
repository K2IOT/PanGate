package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Member;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PresenceUpdate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
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
    for (com.pangate.contracts.v1.PresenceWatch.Entry e : msg.getEntriesList()) {
      if (valid.size() >= max) {
        break;
      }
      if (allowed(viewer, e.getUserId(), e.getConversationId())) {
        valid.add(e);
      }
    }
    long now = wallMs.getAsLong();
    watchers.replace(viewer, device, valid, now + ttlMs);
    Set<String> users = new LinkedHashSet<>();
    valid.forEach(e -> users.add(e.getUserId()));
    if (!users.isEmpty()) {
      PresenceUpdate.Builder u = PresenceUpdate.newBuilder();
      users.forEach(user -> u.addStates(stateFor(user, viewer, now)));
      pusher.send(StatePushes.ephemeral(viewer, device, ChatTopics.TYPE_PRESENCE, u.build().toByteString(),
          StatePushes.PRESENCE_TTL_SEC));
    }
    return valid.size();
  }

  /** {@code GET /v1/presence}: the states of {@code ids} that share {@code convId} with the viewer. */
  public List<PresenceUpdate.State> states(String viewer, String convId, List<String> ids) {
    long now = wallMs.getAsLong();
    List<PresenceUpdate.State> out = new ArrayList<>();
    for (String id : new LinkedHashSet<>(ids)) {
      if (allowed(viewer, id, convId)) {
        out.add(stateFor(id, viewer, now));
      }
    }
    return out;
  }

  boolean allowed(String viewer, String user, String convId) {
    if (user.isEmpty() || user.equals(viewer) || !Ids.isUlid(convId)) {
      return false;
    }
    Optional<ChatStores.Conversation> c = conversations.get(convId);
    if (c.isEmpty() || c.get().kind() == ConversationKind.CONVERSATION_KIND_CHANNEL) {
      return false;
    }
    List<Member> ms = members.of(convId);
    if (ms.stream().noneMatch(m -> m.userId().equals(viewer)) || ms.stream().noneMatch(m -> m.userId().equals(user))) {
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
