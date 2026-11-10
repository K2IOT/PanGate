package com.pangate.router.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.testkit.Fixtures;
import com.pangate.contracts.v1.AckItem.Status;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.OfflineRecord.Reason;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.router.links.NodeUnavailableException;
import com.pangate.router.nodes.Node;
import com.pangate.router.registry.LiveConnection;
import com.pangate.router.registry.RegistryUnavailableException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;

class DeliveryJobTest {

  static final long NOW = Fixtures.FIXED_NOW_MS + 1_000;

  // --- fakes -------------------------------------------------------------------------------------------------
  final List<List<LiveConnection>> rounds = new ArrayList<>();   // live() answers, one per call; last repeats
  int liveCalls;
  final List<String> invalidated = new ArrayList<>();
  RuntimeException registryFailure;
  final Connections conns = new Connections() {
    @Override public List<LiveConnection> live(String p) {
      if (registryFailure != null) {
        throw registryFailure;
      }
      return rounds.isEmpty() ? List.of() : rounds.get(Math.min(liveCalls++, rounds.size() - 1));
    }
    @Override public void invalidate(String p) { invalidated.add(p); }
    @Override public List<LiveConnection> sessions(String p) {
      invalidated.add("sessions:" + p);
      return sessionsAnswer;
    }
  };
  List<LiveConnection> sessionsAnswer = List.of();
  BiFunction<String, PushItem, CompletableFuture<Status>> answer = (n, i) -> done(Status.STATUS_ACKED);
  final List<String> sent = new ArrayList<>();                    // "node/kind/conn"
  final Links links = (node, item) -> {
    sent.add(node + "/" + item.getKind() + "/" + item.getConnId());
    return answer.apply(node, item);
  };
  final Map<String, Node> nodes = new HashMap<>(Map.of(
      "n1", new Node("n1", 1, "a:1", false), "n2", new Node("n2", 1, "a:2", false),
      "nd", new Node("nd", 1, "a:3", true)));
  final List<String> outcomes = new ArrayList<>();
  final OutcomeRecorder recorder = new OutcomeRecorder() {
    @Override public void offline(PushRecord r, Reason reason, int attempts) { outcomes.add("offline " + reason + " " + attempts); }
    @Override public void delivered(PushRecord r, LiveConnection c, Status s) { outcomes.add("delivered " + c.connId() + " " + s); }
  };
  final List<Duration> sleeps = new ArrayList<>();
  long now = NOW;

  DeliveryJob job() {
    Collection<Node> live = nodes.values();
    return new DeliveryJob(conns, links, () -> live, recorder, () -> now, Duration.ofMillis(300),
        List.of(Duration.ofMillis(100), Duration.ofMillis(400)), sleeps::add);
  }

  static CompletableFuture<Status> done(Status s) {
    return CompletableFuture.completedFuture(s);
  }

  static LiveConnection conn(String id, String node) {
    return new LiveConnection(id, node, 1, Optional.of("d-" + id));
  }

  static PushRecord msg() {
    return Fixtures.message("t1:u1");
  }

  // --- MESSAGE ------------------------------------------------------------------------------------------------
  @Test void expiredReliableGoesOfflineExpiredWithZeroAttempts() {
    now = msg().getExpiresAtMs() + 1;
    assertThat(job().run(msg())).isEqualTo(DeliveryResult.OFFLINE);
    assertThat(outcomes).containsExactly("offline REASON_EXPIRED 0");
    assertThat(sent).isEmpty();
  }

  @Test void expiredEphemeralIsDropped() {
    now = msg().getExpiresAtMs() + 1;
    assertThat(job().run(Fixtures.ephemeral("t1:u1"))).isEqualTo(DeliveryResult.DROPPED);
    assertThat(outcomes).isEmpty();
  }

  @Test void noSessionReliableGoesOfflineNoSession() {
    assertThat(job().run(msg())).isEqualTo(DeliveryResult.OFFLINE);
    assertThat(outcomes).containsExactly("offline REASON_NO_SESSION 0");
  }

  @Test void noSessionEphemeralIsDropped() {
    assertThat(job().run(Fixtures.ephemeral("t1:u1"))).isEqualTo(DeliveryResult.DROPPED);
    assertThat(outcomes).isEmpty();
  }

  @Test void ackedWithReceiptRecordsDelivered() {
    rounds.add(List.of(conn("C1", "n1")));
    PushRecord r = msg().toBuilder().setReceipt(true).build();
    assertThat(job().run(r)).isEqualTo(DeliveryResult.DELIVERED);
    assertThat(outcomes).containsExactly("delivered C1 STATUS_ACKED");
    assertThat(sent).containsExactly("n1/KIND_MESSAGE/C1");
  }

  @Test void writtenCountsAsDeliveredAndRecordsWritten() {
    rounds.add(List.of(conn("C1", "n1")));
    answer = (n, i) -> done(Status.STATUS_WRITTEN);
    assertThat(job().run(msg().toBuilder().setReceipt(true).build())).isEqualTo(DeliveryResult.DELIVERED);
    assertThat(outcomes).containsExactly("delivered C1 STATUS_WRITTEN");
  }

  @Test void timeoutRetriesTwiceThenOfflineWithThreeAttempts() {
    rounds.add(List.of(conn("C1", "n1")));
    answer = (n, i) -> done(Status.STATUS_NACK_TIMEOUT);
    assertThat(job().run(msg())).isEqualTo(DeliveryResult.OFFLINE);
    assertThat(outcomes).containsExactly("offline REASON_NACK_TIMEOUT 3");
    assertThat(sleeps).containsExactly(Duration.ofMillis(100), Duration.ofMillis(400));
    assertThat(invalidated).hasSize(2);
  }

  @Test void closedRetriesAndDeliversOnAnotherNode() {
    rounds.add(List.of(conn("C1", "n1")));
    rounds.add(List.of(conn("C2", "n2")));
    answer = (n, i) -> done(n.equals("n1") ? Status.STATUS_NACK_CLOSED : Status.STATUS_ACKED);
    assertThat(job().run(msg())).isEqualTo(DeliveryResult.DELIVERED);
    assertThat(sent).containsExactly("n1/KIND_MESSAGE/C1", "n2/KIND_MESSAGE/C2");
    assertThat(outcomes).isEmpty();
  }

  @Test void overflowShedAndSlowConsumerAreNotRetried() {
    for (Status s : List.of(Status.STATUS_NACK_OVERFLOW, Status.STATUS_NACK_SHED, Status.STATUS_NACK_SLOW_CONSUMER)) {
      outcomes.clear();
      sleeps.clear();
      rounds.clear();
      liveCalls = 0;
      rounds.add(List.of(conn("C1", "n1")));
      answer = (n, i) -> done(s);
      assertThat(job().run(msg())).isEqualTo(DeliveryResult.OFFLINE);
      assertThat(sleeps).as(s.name()).isEmpty();
      assertThat(outcomes).containsExactly("offline " + switch (s) {
        case STATUS_NACK_OVERFLOW -> "REASON_OVERFLOW";
        case STATUS_NACK_SHED -> "REASON_SHED";
        default -> "REASON_SLOW_CONSUMER";
      } + " 1");
    }
  }

  @Test void oneOfTwoConnectionsDeliversSoNoOffline() {
    rounds.add(List.of(conn("C1", "n1"), conn("C2", "n2")));
    answer = (n, i) -> done(n.equals("n1") ? Status.STATUS_NACK_OVERFLOW : Status.STATUS_ACKED);
    assertThat(job().run(msg())).isEqualTo(DeliveryResult.DELIVERED);
    assertThat(outcomes).isEmpty();
  }

  @Test void nodeDiesMidDeliveryOtherConnectionDelivers() {                   // Review Focus 1
    rounds.add(List.of(conn("C1", "n1"), conn("C2", "n2")));
    answer = (n, i) -> {
      if (n.equals("n1")) {
        nodes.remove("n1");
        throw new NodeUnavailableException("n1");
      }
      return done(Status.STATUS_ACKED);
    };
    assertThat(job().run(msg())).isEqualTo(DeliveryResult.DELIVERED);
    assertThat(outcomes).isEmpty();
  }

  @Test void ackNeverArrivesCountsAsTimeoutAfterAckTimeout() {                // Review Focus 2
    rounds.add(List.of(conn("C1", "n1")));
    answer = (n, i) -> new CompletableFuture<>();  // gateway silent
    long t0 = System.nanoTime();
    assertThat(job().run(msg())).isEqualTo(DeliveryResult.OFFLINE);
    assertThat(outcomes).containsExactly("offline REASON_NACK_TIMEOUT 3");
    assertThat(Duration.ofNanos(System.nanoTime() - t0)).isGreaterThanOrEqualTo(Duration.ofMillis(900));
  }

  @Test void lastFailureOnDeadNodeIsNodeGone() {
    rounds.add(List.of(conn("C1", "n1")));
    rounds.add(List.of());
    answer = (n, i) -> {
      nodes.remove("n1");
      return done(Status.STATUS_NACK_CLOSED);
    };
    assertThat(job().run(msg())).isEqualTo(DeliveryResult.OFFLINE);
    assertThat(outcomes).containsExactly("offline REASON_NODE_GONE 1");
  }

  @Test void closedEverywhereEndsAsClosed() {
    rounds.add(List.of(conn("C1", "n1")));
    answer = (n, i) -> done(Status.STATUS_NACK_CLOSED);
    assertThat(job().run(msg())).isEqualTo(DeliveryResult.OFFLINE);
    assertThat(outcomes).containsExactly("offline REASON_CLOSED 3");
  }

  @Test void deviceTargetOnlyUsesMatchingConnections() {
    rounds.add(List.of(new LiveConnection("C1", "n1", 1, Optional.of("d1")),
        new LiveConnection("C2", "n2", 1, Optional.of("d2")), new LiveConnection("C3", "n2", 1, Optional.empty())));
    assertThat(job().run(Fixtures.deviceMessage("t1:u1", "d2"))).isEqualTo(DeliveryResult.DELIVERED);
    assertThat(sent).containsExactly("n2/KIND_MESSAGE/C2");
  }

  @Test void ephemeralCompletesWhenWrittenWithoutRetry() {
    rounds.add(List.of(conn("C1", "n1")));
    answer = (n, i) -> done(Status.STATUS_WRITTEN);
    assertThat(job().run(Fixtures.ephemeral("t1:u1"))).isEqualTo(DeliveryResult.DELIVERED);
    assertThat(outcomes).isEmpty();
    assertThat(sleeps).isEmpty();
  }

  @Test void messageItemsCarryTheFrameAndConnId() {
    rounds.add(List.of(conn("C1", "n1")));
    List<PushItem> items = new ArrayList<>();
    answer = (n, i) -> {
      items.add(i);
      return done(Status.STATUS_ACKED);
    };
    PushRecord r = msg();
    job().run(r);
    PushItem i = items.getFirst();
    assertThat(i.getFrame()).isEqualTo(r.getFrame());
    assertThat(i.getConnId()).isEqualTo("C1");
    assertThat(i.getPrincipalId()).isEqualTo("t1:u1");
    assertThat(i.getTenantId()).isEqualTo("t1");
    assertThat(i.getEnqueuedAtMs()).isEqualTo(r.getAcceptedAtMs());
  }

  // --- BROADCAST / CONTROL / failures -------------------------------------------------------------------------
  @Test void broadcastGoesOnceToEveryLiveNonDrainingNode() {
    assertThat(job().run(Fixtures.broadcast("t1:news"))).isEqualTo(DeliveryResult.DELIVERED);
    assertThat(sent).containsExactlyInAnyOrder("n1/KIND_BROADCAST/", "n2/KIND_BROADCAST/");
    assertThat(outcomes).isEmpty();
  }

  @Test void controlGoesOncePerNodeHoldingThePrincipal() {
    sessionsAnswer = List.of(conn("C1", "n1"), conn("C2", "n1"), conn("C3", "n2"));
    PushRecord c = Fixtures.control("t1:u1", Fixtures.subscribe("t1:news"));
    assertThat(job().run(c)).isEqualTo(DeliveryResult.DELIVERED);
    assertThat(sent).containsExactlyInAnyOrder("n1/KIND_CONTROL/", "n2/KIND_CONTROL/");
  }

  @Test void controlWithoutSessionsIsDone() {
    assertThat(job().run(Fixtures.control("t1:u1", Fixtures.subscribe("t1:news")))).isEqualTo(DeliveryResult.DROPPED);
    assertThat(sent).isEmpty();
  }

  @Test void registryUnavailablePropagates() {
    registryFailure = new RegistryUnavailableException("x", null);
    assertThatThrownBy(() -> job().run(msg())).isInstanceOf(RegistryUnavailableException.class);
  }

  @Test void orderedFrameIsSequencedLikeReliable() {
    rounds.add(List.of(conn("C1", "n1")));
    answer = (n, i) -> done(Status.STATUS_NACK_TIMEOUT);
    PushRecord o = msg().toBuilder().setClass_(DeliveryClass.DELIVERY_CLASS_ORDERED)
        .setFrame(msg().getFrame().toBuilder().setClass_(DeliveryClass.DELIVERY_CLASS_ORDERED)).build();
    assertThat(job().run(o)).isEqualTo(DeliveryResult.OFFLINE);
  }

  /** Shutdown interrupts the worker: the record must stay unfinished (no outcome), so it is redelivered (spec §4). */
  @Test
  void interruptedWhileWaitingRecordsNoOutcome() throws Exception {
    rounds.add(List.of(conn("C1", "n1")));
    answer = (n, i) -> new CompletableFuture<>();
    DeliveryJob j = new DeliveryJob(conns, links, () -> nodes.values(), recorder, () -> now, Duration.ofSeconds(30),
        List.of(Duration.ofMillis(100), Duration.ofMillis(400)), sleeps::add);
    java.util.concurrent.atomic.AtomicReference<Throwable> thrown = new java.util.concurrent.atomic.AtomicReference<>();
    Thread t = Thread.ofVirtual().start(() -> {
      try {
        j.run(msg());
      } catch (Throwable e) {
        thrown.set(e);
      }
    });
    org.awaitility.Awaitility.await().until(() -> !sent.isEmpty());
    t.interrupt();
    t.join(5_000);
    assertThat(t.isAlive()).isFalse();
    assertThat(thrown.get()).isNotNull();
    assertThat(outcomes).isEmpty();
  }

  /** Final review #1: a timed-out wait cancels the future so the link frees the item. */
  @Test
  void timedOutFuturesAreCancelled() {
    rounds.add(List.of(conn("C1", "n1")));
    List<CompletableFuture<Status>> handed = new ArrayList<>();
    answer = (n, i) -> {
      CompletableFuture<Status> f = new CompletableFuture<>();
      handed.add(f);
      return f;
    };
    job().run(msg());
    assertThat(handed).hasSize(3).allMatch(CompletableFuture::isCancelled);
  }

  /** Final review #3: an interrupt (shutdown) never ends in an outcome, even after an already completed NACK. */
  @Test
  void interruptedBeforeTheOutcomeWritesNothing() throws Exception {
    rounds.add(List.of(conn("C1", "n1")));
    answer = (n, i) -> done(Status.STATUS_NACK_CLOSED);
    java.util.concurrent.atomic.AtomicReference<Throwable> thrown = new java.util.concurrent.atomic.AtomicReference<>();
    Thread t = Thread.ofVirtual().start(() -> {
      Thread.currentThread().interrupt();
      try {
        job().run(msg());
      } catch (Throwable e) {
        thrown.set(e);
      }
    });
    t.join(5_000);
    assertThat(thrown.get()).isInstanceOf(IllegalStateException.class);
    assertThat(outcomes).isEmpty();
  }

  /** Final review #6: OVERFLOW / SHED / SLOW_CONSUMER connections are not retried (contracts §1.1a). */
  @Test
  void retryRoundsSkipConnectionsThatFailedWithoutRetry() {
    rounds.add(List.of(conn("C1", "n1"), conn("C2", "n2")));
    answer = (n, i) -> done(i.getConnId().equals("C1") ? Status.STATUS_NACK_OVERFLOW : Status.STATUS_NACK_CLOSED);
    assertThat(job().run(msg())).isEqualTo(DeliveryResult.OFFLINE);
    assertThat(sent).containsExactly("n1/KIND_MESSAGE/C1", "n2/KIND_MESSAGE/C2", "n2/KIND_MESSAGE/C2",
        "n2/KIND_MESSAGE/C2");
    assertThat(outcomes).containsExactly("offline REASON_CLOSED 4");
  }

  /** Final review #7: KICK reads the registry fresh and includes draining nodes. */
  @Test
  void controlReadsFreshSessionsIncludingDrainingNodes() {
    sessionsAnswer = List.of(conn("C1", "n1"), conn("C9", "nd"));
    PushRecord c = Fixtures.control("t1:u1", Fixtures.subscribe("t1:news"));
    assertThat(job().run(c)).isEqualTo(DeliveryResult.DELIVERED);
    assertThat(invalidated).containsExactly("t1:u1", "sessions:t1:u1");
    assertThat(sent).containsExactlyInAnyOrder("n1/KIND_CONTROL/", "nd/KIND_CONTROL/");
  }

  /**
   * Single-session KICK: the registry already holds only the new entry (the gateway replaced the old one), so KICK
   * cannot be routed by entry; it goes to every live node, draining ones too (push-api P1, PushApiIT).
   */
  @Test
  void kickGoesToEveryLiveNodeIncludingDraining() {
    sessionsAnswer = List.of(conn("NEW", "n2"));
    PushRecord c = Fixtures.control("t1:u1", Fixtures.kick("NEW", 1));
    assertThat(job().run(c)).isEqualTo(DeliveryResult.DELIVERED);
    assertThat(sent).containsExactlyInAnyOrder("n1/KIND_CONTROL/", "n2/KIND_CONTROL/", "nd/KIND_CONTROL/");
  }

  /** Final review minor: a KICK goes to every node, so it must not wait for the slowest one (ack timeout 30 s here). */
  @Test
  void kickDoesNotWaitForASilentNode() {
    answer = (n, i) -> n.equals("n1") ? new CompletableFuture<>() : done(Status.STATUS_ACKED);
    DeliveryJob j = new DeliveryJob(conns, links, () -> nodes.values(), recorder, () -> now, Duration.ofSeconds(30),
        List.of(Duration.ofMillis(100), Duration.ofMillis(400)), sleeps::add);
    PushRecord c = Fixtures.control("t1:u1", Fixtures.kick("NEW", 1));
    assertThat(org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(Duration.ofSeconds(2), () -> j.run(c)))
        .isEqualTo(DeliveryResult.DELIVERED);
    assertThat(sent).hasSize(3);
  }

  /** Flow review (contracts §1.1a): an expired BROADCAST is dropped, not delivered late. */
  @Test
  void expiredBroadcastIsDropped() {
    PushRecord b = Fixtures.broadcast("t1:news");
    now = b.getExpiresAtMs() + 1;
    assertThat(job().run(b)).isEqualTo(DeliveryResult.DROPPED);
    assertThat(sent).isEmpty();
  }

  /** Flow review (contracts §1.1a): an expired CONTROL is dropped too. */
  @Test
  void expiredControlIsDropped() {
    PushRecord c = Fixtures.control("t1:u1", Fixtures.kick("NEW", 1));
    now = c.getExpiresAtMs() + 1;
    assertThat(job().run(c)).isEqualTo(DeliveryResult.DROPPED);
    assertThat(sent).isEmpty();
  }
}
