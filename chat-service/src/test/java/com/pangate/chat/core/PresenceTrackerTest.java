package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.EventType;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** CH3 spec §6.1: online from gateway lifecycle events, offline only after a 5 s grace. */
class PresenceTrackerTest {

  final FakePresence state = new FakePresence();
  final FakeStores durable = new FakeStores();
  final List<PresenceTracker.Transition> out = new CopyOnWriteArrayList<>();
  final AtomicLong now = new AtomicLong(1_000_000);
  final PresenceTracker tracker = new PresenceTracker(state, durable, out::add, now::get, Duration.ofSeconds(5));

  static ConnectionEvent ev(EventType t, String user, String node, String conn) {
    return ConnectionEvent.newBuilder().setType(t).setPrincipalId("chat:" + user).setNodeId(node).setEpoch(1)
        .setConnId(conn).setTenantId("chat").setEventId(Ids.next()).build();
  }

  void at(long ms) {
    now.set(ms);
    tracker.tick();
  }

  @Test
  void connectMakesOnline() {
    tracker.on(ev(EventType.EVENT_TYPE_CONNECTED, "a", "n1", "c1"));
    assertThat(out).containsExactly(new PresenceTracker.Transition("a", true, 1_000_000));
    assertThat(state.online("a")).isTrue();
  }

  @Test
  void disconnectGoesOfflineAfterGrace() {
    tracker.on(ev(EventType.EVENT_TYPE_CONNECTED, "a", "n1", "c1"));
    out.clear();
    tracker.on(ev(EventType.EVENT_TYPE_DISCONNECTED, "a", "n1", "c1"));
    at(1_004_900);
    assertThat(out).isEmpty();
    at(1_005_000);
    assertThat(out).containsExactly(new PresenceTracker.Transition("a", false, 1_000_000));
    assertThat(state.lastSeen("a")).hasValue(1_000_000);
    assertThat(durable.lastSeen("a")).hasValue(1_000_000);
    at(1_010_000);
    assertThat(out).hasSize(1);
  }

  @Test
  void reconnectWithinGraceEmitsNothing() {
    tracker.on(ev(EventType.EVENT_TYPE_CONNECTED, "a", "n1", "c1"));
    out.clear();
    tracker.on(ev(EventType.EVENT_TYPE_HEARTBEAT_TIMEOUT, "a", "n1", "c1"));
    now.set(1_002_000);
    tracker.on(ev(EventType.EVENT_TYPE_CONNECTED, "a", "n2", "c2"));
    at(1_010_000);
    assertThat(out).isEmpty();
    assertThat(state.online("a")).isTrue();
  }

  /** Review Focus 4. */
  @Test
  void secondDeviceKeepsOnline() {
    tracker.on(ev(EventType.EVENT_TYPE_CONNECTED, "a", "n1", "c1"));
    tracker.on(ev(EventType.EVENT_TYPE_CONNECTED, "a", "n1", "c2"));
    out.clear();
    tracker.on(ev(EventType.EVENT_TYPE_KICKED, "a", "n1", "c1"));
    at(1_010_000);
    assertThat(out).isEmpty();
    assertThat(state.online("a")).isTrue();
  }

  @Test
  void nodeDownDropsItsConnections() {
    tracker.on(ev(EventType.EVENT_TYPE_CONNECTED, "a", "n1", "c1"));
    tracker.on(ev(EventType.EVENT_TYPE_CONNECTED, "b", "n2", "c2"));
    out.clear();
    tracker.on(ConnectionEvent.newBuilder().setType(EventType.EVENT_TYPE_NODE_DOWN).setNodeId("n1").setEpoch(1)
        .setEventId(Ids.next()).build());
    at(1_005_000);
    assertThat(out).containsExactly(new PresenceTracker.Transition("a", false, 1_000_000));
    assertThat(state.online("b")).isTrue();
  }

  /** Review Focus 5. */
  @Test
  void nodeDownOfOldNodeAfterReconnectStaysOnline() {
    tracker.on(ev(EventType.EVENT_TYPE_CONNECTED, "a", "n1", "c1"));
    tracker.on(ev(EventType.EVENT_TYPE_CONNECTED, "a", "n2", "c2"));
    out.clear();
    tracker.on(ConnectionEvent.newBuilder().setType(EventType.EVENT_TYPE_NODE_DOWN).setNodeId("n1").setEpoch(1)
        .setEventId(Ids.next()).build());
    at(1_010_000);
    assertThat(out).isEmpty();
    assertThat(state.online("a")).isTrue();
  }

  @Test
  void duplicateEventIdIgnored() {
    ConnectionEvent c = ev(EventType.EVENT_TYPE_CONNECTED, "a", "n1", "c1");
    tracker.on(c);
    tracker.on(ev(EventType.EVENT_TYPE_DISCONNECTED, "a", "n1", "c1"));
    tracker.on(c);   // replayed after the disconnect: must not bring a back
    at(1_005_000);
    assertThat(state.online("a")).isFalse();
    assertThat(out).extracting(PresenceTracker.Transition::online).containsExactly(true, false);
  }

  @Test
  void nonChatPrincipalIgnored() {
    tracker.on(ConnectionEvent.newBuilder().setType(EventType.EVENT_TYPE_CONNECTED).setPrincipalId("t1:x")
        .setNodeId("n1").setConnId("c").setEventId(Ids.next()).build());
    assertThat(out).isEmpty();
    assertThat(state.conns).isEmpty();
  }

  /** Final review I6: graces live in the shared state, so another pod (or a restarted one) finishes them. */
  @Test
  void graceSurvivesARestart() {
    tracker.on(ev(EventType.EVENT_TYPE_CONNECTED, "a", "n1", "c1"));
    tracker.on(ev(EventType.EVENT_TYPE_DISCONNECTED, "a", "n1", "c1"));
    List<PresenceTracker.Transition> other = new CopyOnWriteArrayList<>();
    PresenceTracker restarted = new PresenceTracker(state, durable, other::add, now::get, Duration.ofSeconds(5));
    now.set(1_005_000);
    restarted.tick();
    assertThat(other).containsExactly(new PresenceTracker.Transition("a", false, 1_000_000));
  }

  /** Final review I6: a grace whose work failed is retried, not dropped. */
  @Test
  void failedGraceWorkIsRetried() {
    tracker.on(ev(EventType.EVENT_TYPE_CONNECTED, "a", "n1", "c1"));
    out.clear();
    tracker.on(ev(EventType.EVENT_TYPE_DISCONNECTED, "a", "n1", "c1"));
    state.failLastSeenWrites = 1;
    at(1_005_000);
    assertThat(out).isEmpty();
    at(1_007_000);
    assertThat(out).containsExactly(new PresenceTracker.Transition("a", false, 1_000_000));
  }

  /** Final review I6: a gone mark older than the grace (its offline was lost) does not swallow the next online. */
  @Test
  void staleGoneMarkOnConnectStillEmitsOnline() {
    state.markGone("a", 1_000_000 - 600_000);
    tracker.on(ev(EventType.EVENT_TYPE_CONNECTED, "a", "n1", "c1"));
    assertThat(out).containsExactly(new PresenceTracker.Transition("a", true, 1_000_000));
    assertThat(state.lastSeen("a")).hasValue(1_000_000 - 600_000);
  }

  /** Final review I7: NODE_DOWN handled before a late CONNECTED of that node leaves no zombie online. */
  @Test
  void nodeDownBeforeALateConnectedLeavesNoZombie() {
    tracker.on(ConnectionEvent.newBuilder().setType(EventType.EVENT_TYPE_NODE_DOWN).setNodeId("n1").setEpoch(1)
        .setEventId(Ids.next()).build());
    tracker.on(ev(EventType.EVENT_TYPE_CONNECTED, "a", "n1", "c1"));
    assertThat(state.online("a")).isFalse();
    assertThat(out).isEmpty();
  }
}
