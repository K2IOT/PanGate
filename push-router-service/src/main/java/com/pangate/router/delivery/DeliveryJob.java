package com.pangate.router.delivery;

import com.pangate.contracts.v1.AckItem.Status;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.OfflineRecord.Reason;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.router.links.NodeUnavailableException;
import com.pangate.router.nodes.Node;
import com.pangate.router.registry.LiveConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * One PushRecord from start to outcome (push-router spec §2, push contracts spec §1.1a). Blocking by design: it runs on
 * a Parallel Consumer virtual-thread worker and returns only when the record is finished.
 */
public final class DeliveryJob {

  private final Connections conns;
  private final Links links;
  private final Supplier<Collection<Node>> liveNodes;
  private final OutcomeRecorder recorder;
  private final LongSupplier wallMs;
  private final Duration ackTimeout;
  private final List<Duration> retries;
  private final Sleeper sleeper;

  public DeliveryJob(Connections conns, Links links, Supplier<Collection<Node>> liveNodes, OutcomeRecorder recorder,
      LongSupplier wallMs, Duration ackTimeout, List<Duration> retries, Sleeper sleeper) {
    this.conns = conns;
    this.links = links;
    this.liveNodes = liveNodes;
    this.recorder = recorder;
    this.wallMs = wallMs;
    this.ackTimeout = ackTimeout;
    this.retries = List.copyOf(retries);
    this.sleeper = sleeper;
  }

  public DeliveryResult run(PushRecord r) {
    if (r.getKind() != PushRecord.Kind.KIND_MESSAGE && wallMs.getAsLong() > r.getExpiresAtMs()) {
      return DeliveryResult.DROPPED;   // contracts §1.1a: expired BROADCAST / CONTROL are dropped (MESSAGE: below)
    }
    DeliveryResult result = switch (r.getKind()) {
      case KIND_MESSAGE -> message(r);
      case KIND_BROADCAST -> broadcast(r);
      case KIND_CONTROL -> control(r);
      default -> DeliveryResult.DROPPED;
    };
    checkInterrupted();   // a record finished while shutting down is still left for the next owner
    return result;
  }

  private static void checkInterrupted() {
    if (Thread.currentThread().isInterrupted()) {
      throw new IllegalStateException("interrupted: record left unfinished");
    }
  }

  static boolean sequenced(PushRecord r) {
    return r.getClass_() == DeliveryClass.DELIVERY_CLASS_RELIABLE
        || r.getClass_() == DeliveryClass.DELIVERY_CLASS_ORDERED;
  }

  private boolean nodeLive(String nodeId) {
    return liveNodes.get().stream().anyMatch(n -> n.id().equals(nodeId));
  }

  // --- MESSAGE -------------------------------------------------------------------------------------------------

  private DeliveryResult message(PushRecord r) {
    boolean device = r.getTargetCase() == PushRecord.TargetCase.DEVICE;
    String principal = device ? r.getDevice().getPrincipalId() : r.getPrincipalId();
    Optional<String> deviceId = device ? Optional.of(r.getDevice().getDeviceId()) : Optional.empty();
    boolean seq = sequenced(r);
    if (wallMs.getAsLong() > r.getExpiresAtMs()) {
      return finish(r, seq, Reason.REASON_EXPIRED, 0);
    }
    int attempts = 0;
    Reason last = Reason.REASON_NO_SESSION;
    java.util.Set<String> failedFinally = new java.util.HashSet<>();   // OVERFLOW / SHED / SLOW_CONSUMER: no retry
    for (int round = 0; round <= retries.size(); round++) {
      if (round > 0) {
        pause(retries.get(round - 1));
        conns.invalidate(principal);
      }
      List<LiveConnection> targets = conns.live(principal).stream()
          .filter(c -> deviceId.isEmpty() || deviceId.equals(c.device().filter(d -> !d.isEmpty())))
          .filter(c -> !failedFinally.contains(c.connId())).toList();
      if (targets.isEmpty()) {
        if (last != Reason.REASON_NODE_GONE) {
          last = Reason.REASON_NO_SESSION;
        }
        break;
      }
      List<CompletableFuture<Status>> futures = new ArrayList<>(targets.size());
      for (LiveConnection c : targets) {
        futures.add(sendTo(c.nodeId(), messageItem(r, c)));
        attempts++;
      }
      boolean delivered = false;
      boolean retryable = false;
      Reason roundReason = null;
      long deadline = System.nanoTime() + ackTimeout.toNanos();
      for (int i = 0; i < targets.size(); i++) {
        LiveConnection c = targets.get(i);
        Status s = await(futures.get(i), deadline);
        switch (s) {
          case STATUS_ACKED, STATUS_WRITTEN -> {
            delivered = true;
            if (r.getReceipt() && seq) {
              checkInterrupted();
              recorder.delivered(r, c, s);
            }
          }
          case STATUS_NACK_TIMEOUT -> {
            retryable = true;
            roundReason = Reason.REASON_NACK_TIMEOUT;
          }
          case STATUS_NACK_CLOSED, STATUS_NACK_NOT_FOUND -> {
            retryable = true;
            if (roundReason != Reason.REASON_NACK_TIMEOUT) {
              roundReason = nodeLive(c.nodeId()) ? Reason.REASON_CLOSED : Reason.REASON_NODE_GONE;
            }
          }
          case STATUS_NACK_OVERFLOW -> {
            failedFinally.add(c.connId());
            roundReason = orKeep(roundReason, Reason.REASON_OVERFLOW);
          }
          case STATUS_NACK_SLOW_CONSUMER -> {
            failedFinally.add(c.connId());
            roundReason = orKeep(roundReason, Reason.REASON_SLOW_CONSUMER);
          }
          case STATUS_NACK_SHED -> {
            failedFinally.add(c.connId());
            roundReason = orKeep(roundReason, Reason.REASON_SHED);
          }
          default -> roundReason = orKeep(roundReason, Reason.REASON_CLOSED);
        }
      }
      if (delivered) {
        return DeliveryResult.DELIVERED;
      }
      if (!seq) {
        return DeliveryResult.DROPPED;  // EPHEMERAL: no ACK expected, no retry, no offline
      }
      last = roundReason == null ? Reason.REASON_CLOSED : roundReason;
      if (!retryable) {
        break;
      }
    }
    return finish(r, seq, last, attempts);
  }

  private static Reason orKeep(Reason current, Reason candidate) {
    return current == null ? candidate : current;
  }

  private DeliveryResult finish(PushRecord r, boolean seq, Reason reason, int attempts) {
    checkInterrupted();
    if (!seq) {
      return DeliveryResult.DROPPED;
    }
    recorder.offline(r, reason, attempts);
    return DeliveryResult.OFFLINE;
  }

  private static PushItem messageItem(PushRecord r, LiveConnection c) {
    return PushItem.newBuilder().setKind(PushItem.Kind.KIND_MESSAGE).setTenantId(r.getTenantId())
        .setPrincipalId(r.getTargetCase() == PushRecord.TargetCase.DEVICE ? r.getDevice().getPrincipalId()
            : r.getPrincipalId())
        .setConnId(c.connId()).setFrame(r.getFrame()).setPriority(r.getPriority())
        .setEnqueuedAtMs(r.getAcceptedAtMs()).build();
  }

  // --- BROADCAST / CONTROL -----------------------------------------------------------------------------------

  private DeliveryResult broadcast(PushRecord r) {
    PushItem item = PushItem.newBuilder().setKind(PushItem.Kind.KIND_BROADCAST).setTenantId(r.getTenantId())
        .setTopic(r.getTopic()).setFrame(r.getFrame()).setPriority(r.getPriority())
        .setEnqueuedAtMs(r.getAcceptedAtMs()).build();
    List<CompletableFuture<Status>> written = new ArrayList<>();
    for (Node n : liveNodes.get()) {
      if (!n.draining()) {
        written.add(sendTo(n.id(), item));
      }
    }
    awaitAll(written);
    return DeliveryResult.DELIVERED;
  }

  private DeliveryResult control(PushRecord r) {
    Set<String> nodes = new LinkedHashSet<>();
    if (r.getControl().hasKick()) {
      // single-session KICK: the gateway already replaced the old entry with the new one in reg:{p}, so the node
      // holding the older session is not in the registry any more; every live node (draining too) gets the KICK and
      // closes only sessions older than new_conn_id
      liveNodes.get().forEach(n -> nodes.add(n.id()));
    } else {
      // read the registry fresh, and reach draining nodes too (spec §2)
      conns.invalidate(r.getPrincipalId());
      conns.sessions(r.getPrincipalId()).forEach(c -> nodes.add(c.nodeId()));
    }
    if (nodes.isEmpty()) {
      return DeliveryResult.DROPPED;
    }
    PushItem item = PushItem.newBuilder().setKind(PushItem.Kind.KIND_CONTROL).setTenantId(r.getTenantId())
        .setPrincipalId(r.getPrincipalId()).setControl(r.getControl()).setPriority(r.getPriority())
        .setEnqueuedAtMs(r.getAcceptedAtMs()).build();
    List<CompletableFuture<Status>> written = new ArrayList<>();
    nodes.forEach(n -> written.add(sendTo(n, item)));
    if (!r.getControl().hasKick()) {
      awaitAll(written);
    }
    // a KICK reaches every live node, and its ack changes nothing (no retry, no outcome): waiting would hold the
    // partition for the slowest node in the fleet; each link still writes it in order before later items
    return DeliveryResult.DELIVERED;
  }

  // --- helpers -------------------------------------------------------------------------------------------------

  private CompletableFuture<Status> sendTo(String nodeId, PushItem item) {
    try {
      return links.send(nodeId, item);
    } catch (NodeUnavailableException e) {
      return CompletableFuture.completedFuture(Status.STATUS_NACK_CLOSED);
    }
  }

  private void awaitAll(List<CompletableFuture<Status>> futures) {
    long deadline = System.nanoTime() + ackTimeout.toNanos();
    futures.forEach(f -> await(f, deadline));
  }

  /** The future's status, or NACK_TIMEOUT once {@code deadline} (nanoTime) passed. */
  private static Status await(CompletableFuture<Status> f, long deadline) {
    try {
      return f.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
    } catch (TimeoutException e) {
      f.cancel(false);   // the link frees the item and its permit (final review #1)
      return Status.STATUS_NACK_TIMEOUT;
    } catch (InterruptedException e) {
      throw interrupted(e);
    } catch (Exception e) {
      return Status.STATUS_NACK_CLOSED;
    }
  }

  private void pause(Duration d) {
    try {
      sleeper.sleep(d);
    } catch (InterruptedException e) {
      throw interrupted(e);
    }
  }

  /** Shutdown: leave the record unfinished (no outcome) so it is processed again after restart (spec §4). */
  private static IllegalStateException interrupted(InterruptedException e) {
    Thread.currentThread().interrupt();
    return new IllegalStateException("interrupted: record left unfinished", e);
  }
}
