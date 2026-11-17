package com.pangate.chat.core;

import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.EventType;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * CH3 spec §6.1–6.2: online from {@code c1.gw.lifecycle}. Empty → not empty is online at once (unless the user was
 * only inside the offline grace); the last connection closing starts a grace ({@code offlineGrace}, 5 s) and only if
 * the user is still without a connection then is it offline, with {@code last_seen} = when the last one closed.
 * {@link #tick()} ends graces; call it often (every 500 ms). Grace marks live in the shared state, so a restarted pod
 * only delays an offline until the next transition of that user.
 */
public final class PresenceTracker {

  public record Transition(String userId, boolean online, long atMs) {}

  private final PresenceState state;
  private final ChatStores.LastSeenStore durable;
  private final Consumer<Transition> onTransition;
  private final LongSupplier wallMs;
  private final long graceMs;
  /** user → when its grace ends (this pod's timer). */
  private final Map<String, Long> pending = new ConcurrentHashMap<>();

  public PresenceTracker(PresenceState state, ChatStores.LastSeenStore durable, Consumer<Transition> onTransition,
      LongSupplier wallMs, Duration grace) {
    this.state = state;
    this.durable = durable;
    this.onTransition = onTransition;
    this.wallMs = wallMs;
    this.graceMs = grace.toMillis();
  }

  public void on(ConnectionEvent e) {
    if (!e.getEventId().isEmpty() && state.seen(e.getEventId())) {
      return;
    }
    long at = e.getAtMs() > 0 ? e.getAtMs() : wallMs.getAsLong();
    if (e.getType() == EventType.EVENT_TYPE_NODE_DOWN) {
      for (String user : state.dropNode(e.getNodeId(), e.getEpoch())) {
        gone(user, at);
      }
      return;
    }
    Optional<String> user = UserIds.fromPrincipal(e.getPrincipalId());
    if (user.isEmpty()) {
      return;
    }
    switch (e.getType()) {
      case EVENT_TYPE_CONNECTED -> {
        if (state.add(user.get(), e.getNodeId(), e.getEpoch(), e.getConnId())) {
          pending.remove(user.get());
          if (state.gone(user.get()).isPresent()) {
            state.clearGone(user.get());   // back inside the grace: nobody saw it go
          } else {
            onTransition.accept(new Transition(user.get(), true, at));
          }
        }
      }
      case EVENT_TYPE_DISCONNECTED, EVENT_TYPE_HEARTBEAT_TIMEOUT, EVENT_TYPE_KICKED -> {
        if (state.remove(user.get(), e.getNodeId(), e.getEpoch(), e.getConnId())) {
          gone(user.get(), at);
        }
      }
      default -> {
        // other events do not change presence
      }
    }
  }

  private void gone(String user, long at) {
    state.markGone(user, at);
    pending.put(user, wallMs.getAsLong() + graceMs);
  }

  /** Ends the graces that are due. */
  public void tick() {
    long now = wallMs.getAsLong();
    for (Iterator<Map.Entry<String, Long>> it = pending.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<String, Long> p = it.next();
      if (now < p.getValue()) {
        continue;
      }
      it.remove();
      String user = p.getKey();
      OptionalLong goneAt = state.gone(user);
      if (goneAt.isEmpty() || state.online(user)) {
        continue;
      }
      state.putLastSeen(user, goneAt.getAsLong());
      durable.putLastSeen(user, goneAt.getAsLong());
      state.clearGone(user);
      onTransition.accept(new Transition(user, false, goneAt.getAsLong()));
    }
  }
}
