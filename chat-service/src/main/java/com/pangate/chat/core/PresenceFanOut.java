package com.pangate.chat.core;

import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.PresenceUpdate;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * CH3 spec §6.3: an online/offline transition, pushed to every device watching that user. Nothing goes to a pair
 * that may not see presence (either side hides it, spec §8): a push of any content would time the transition.
 */
public final class PresenceFanOut implements Consumer<PresenceTracker.Transition> {

  private final Watchers watchers;
  private final MemberLookup members;
  private final PresenceState presence;
  private final ChatStores.LastSeenStore durable;
  private final PrivacyCache privacy;
  private final Pusher pusher;
  private final LongSupplier wallMs;

  public PresenceFanOut(Watchers watchers, MemberLookup members, PresenceState presence,
      ChatStores.LastSeenStore durable, PrivacyCache privacy, Pusher pusher, LongSupplier wallMs) {
    this.watchers = watchers;
    this.members = members;
    this.presence = presence;
    this.durable = durable;
    this.privacy = privacy;
    this.pusher = pusher;
    this.wallMs = wallMs;
  }

  @Override
  public void accept(PresenceTracker.Transition t) {
    on(t);
  }

  /** Returns how many devices were told. */
  public int on(PresenceTracker.Transition t) {
    long now = wallMs.getAsLong();
    int n = 0;
    ChatStores.Privacy subject = privacy.of(t.userId());
    java.util.Map<String, java.util.Set<String>> byConv = new java.util.HashMap<>();
    for (String w : watchers.watchersOf(t.userId(), now)) {
      String[] parts = w.split("/", 3);
      // Codex review of PR #6: a watch outlives a membership change by up to its TTL; the conversation must still
      // hold both
      java.util.Set<String> ms = byConv.computeIfAbsent(parts[2], c -> {
        java.util.Set<String> ids = new java.util.HashSet<>();
        members.of(c).forEach(m -> ids.add(m.userId()));
        return ids;
      });
      if (!ms.contains(parts[0]) || !ms.contains(t.userId())) {
        continue;
      }
      ChatStores.Privacy viewer = privacy.of(parts[0]);
      if (!PrivacyRules.mayShowPresence(subject, viewer)) {
        continue;   // final review C2: even a "hidden" push would time the transition
      }
      PresenceUpdate.State s = PresenceView.state(t.userId(), t.online(),
          t.online() ? OptionalLong.empty() : OptionalLong.of(t.atMs()), subject, viewer, now);
      pusher.send(StatePushes.ephemeral(parts[0], parts[1], ChatTopics.TYPE_PRESENCE,
          PresenceUpdate.newBuilder().addStates(s).build().toByteString(), StatePushes.PRESENCE_TTL_SEC),
          Pusher.Priority.DROPPABLE);
      n++;
    }
    return n;
  }
}
