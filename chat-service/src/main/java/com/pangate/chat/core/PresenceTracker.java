package com.pangate.chat.core;

import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.EventType;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * CH3 spec §6.1–6.2: online from {@code c1.gw.lifecycle}. Empty → not empty is online at once (unless the user was
 * only inside the offline grace); the last connection closing starts a grace ({@code offlineGrace}, 5 s) and only if
 * the user is still without a connection then is it offline, with {@code last_seen} = when the last one closed.
 * {@link #tick()} ends due graces; call it often (every 500 ms). Graces are a queue in the shared state, so any pod
 * finishes them and a failed one is retried; a CONNECTED after an expired grace mark still says online.
 */
public final class PresenceTracker {

  public record Transition(String userId, boolean online, long atMs) {}

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(PresenceTracker.class);
  static final long RETRY_MS = 1_000;

  private final PresenceState state;
  private final ChatStores.LastSeenStore durable;
  private final Consumer<Transition> onTransition;
  private final LongSupplier wallMs;
  private final long graceMs;

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
      case EVENT_TYPE_CONNECTED -> connected(user.get(), e, at);
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

  private void connected(String user, ConnectionEvent e, long at) {
    if (!state.add(user, e.getNodeId(), e.getEpoch(), e.getConnId())) {
      return;
    }
    state.cancelGrace(user);
    OptionalLong goneAt = state.gone(user);
    if (goneAt.isPresent()) {
      state.clearGone(user);
      if (at - goneAt.getAsLong() <= graceMs) {
        return;   // back inside the grace: nobody saw it go
      }
      // final review I6: an expired mark means its offline was lost; record when it went, then say online
      state.putLastSeen(user, goneAt.getAsLong());
      durable.putLastSeen(user, goneAt.getAsLong());
    }
    onTransition.accept(new Transition(user, true, at));
  }

  private void gone(String user, long at) {
    state.markGone(user, at);
    state.scheduleGrace(user, wallMs.getAsLong() + graceMs);
  }

  /** Ends the graces that are due, on any pod; a failed one is put back and retried. */
  public void tick() {
    long now = wallMs.getAsLong();
    for (String user : state.dueGraces(now)) {
      if (!state.claimGrace(user)) {
        continue;
      }
      try {
        OptionalLong goneAt = state.gone(user);
        if (goneAt.isEmpty() || state.online(user)) {
          continue;
        }
        state.putLastSeen(user, goneAt.getAsLong());
        durable.putLastSeen(user, goneAt.getAsLong());
        state.clearGone(user);
        onTransition.accept(new Transition(user, false, goneAt.getAsLong()));
      } catch (RuntimeException ex) {
        log.debug("presence grace of {} failed, retried: {}", user, ex.toString());
        try {
          state.scheduleGrace(user, now + RETRY_MS);
        } catch (RuntimeException again) {
          // the state is down: the grace mark stays and the next connect or transition settles it
        }
      }
    }
  }
}
