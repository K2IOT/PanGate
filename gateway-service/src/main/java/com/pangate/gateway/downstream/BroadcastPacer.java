package com.pangate.gateway.downstream;

import com.pangate.contracts.v1.PushItem.Priority;
import com.pangate.gateway.config.LimitsHolder;
import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import io.micrometer.core.instrument.Counter;
import io.netty.channel.Channel;
import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Broadcast pacing of one event loop (spec §6.4). CRITICAL is written at once; NORMAL then BULK are written at most
 * {@code broadcastRatePerLoop / 100} recipients per 10 ms. BULK waits while shedding (level ≥ 1). The timer exists
 * only while jobs are queued. Unicast never waits behind it: LoopBatchTask does not go through the pacer.
 */
public final class BroadcastPacer {

  /**
   * Queued NORMAL + BULK jobs per loop. Each pins a direct buffer and a recipient snapshot: past the cap the oldest
   * BULK, then the oldest NORMAL, is dropped ({@code gw.push.dropped{reason=broadcast_backlog}}).
   */
  public static final int MAX_QUEUED_JOBS = 256;
  private static final long TICK_MS = 10;
  private static final long SHED_RECHECK_MS = 100;

  private static final class Job {
    final PreEncodedFrame frame;
    final List<Session> recipients;
    final Counter counter;
    int next;

    Job(PreEncodedFrame frame, List<Session> recipients, Counter counter) {
      this.frame = frame;
      this.recipients = recipients;
      this.counter = counter;
    }
  }

  private final LoopContext loop;
  private final NodeState node;
  private final TenantLimitsStore tenants;
  private final LimitsHolder limits;
  private final ArrayDeque<Job> normal = new ArrayDeque<>();
  private final ArrayDeque<Job> bulk = new ArrayDeque<>();
  private final Counter critical;
  private final Counter normalCount;
  private final Counter bulkCount;
  private final Counter disabled;
  private final Counter backlog;
  private boolean scheduled;
  private volatile int queued;  // written on the loop only; read by the gw.broadcast.queued gauge

  public BroadcastPacer(LoopContext loop, NodeState node, TenantLimitsStore tenants, LimitsHolder limits,
      GatewayMetrics m) {
    this.loop = loop;
    this.node = node;
    this.tenants = tenants;
    this.limits = limits;
    this.critical = m.counter("gw.broadcast.recipients", "priority", "critical");
    this.normalCount = m.counter("gw.broadcast.recipients", "priority", "normal");
    this.bulkCount = m.counter("gw.broadcast.recipients", "priority", "bulk");
    this.disabled = m.counter("gw.push.dropped", "reason", "broadcast_disabled");
    this.backlog = m.counter("gw.push.dropped", "reason", "broadcast_backlog");
  }

  /** On the loop; takes ownership of exactly one share of {@code frame}. */
  public void submit(String topic, Priority p, PreEncodedFrame frame) {
    if (p != Priority.PRIORITY_CRITICAL && !tenants.policy(frame.tenantId()).broadcastEnabled()) {
      disabled.increment();
      frame.releaseShare();
      return;
    }
    List<Session> recipients = loop.topics().snapshot(topic);
    if (recipients.isEmpty()) {
      frame.releaseShare();
      return;
    }
    if (p == Priority.PRIORITY_CRITICAL) {
      Job job = new Job(frame, recipients, critical);
      ReferenceOpenHashSet<Channel> written = new ReferenceOpenHashSet<>();
      write(job, Integer.MAX_VALUE, written);
      written.forEach(Channel::flush);
      frame.releaseShare();
      return;
    }
    if (normal.size() + bulk.size() >= MAX_QUEUED_JOBS) {
      Job oldest = !bulk.isEmpty() ? bulk.poll() : normal.poll();
      oldest.frame.releaseShare();
      backlog.increment();
    }
    if (p == Priority.PRIORITY_BULK) {
      bulk.add(new Job(frame, recipients, bulkCount));
    } else {
      normal.add(new Job(frame, recipients, normalCount));
    }
    queued = normal.size() + bulk.size();
    schedule(TICK_MS);
  }

  private void schedule(long ms) {
    if (!scheduled) {
      scheduled = true;
      loop.eventLoop().schedule(this::tick, ms, TimeUnit.MILLISECONDS);
    }
  }

  void tick() {
    scheduled = false;
    int budget = Math.max(1, limits.current().broadcastRatePerLoop() / 100);
    boolean shedding = node.shedLevel() >= 1;
    ReferenceOpenHashSet<Channel> written = new ReferenceOpenHashSet<>();
    while (budget > 0) {
      ArrayDeque<Job> q = !normal.isEmpty() ? normal : (!shedding && !bulk.isEmpty() ? bulk : null);
      if (q == null) {
        break;
      }
      Job job = q.peek();
      budget -= write(job, budget, written);
      if (job.next >= job.recipients.size()) {
        q.poll();
        job.frame.releaseShare();
      }
    }
    written.forEach(Channel::flush);
    queued = normal.size() + bulk.size();
    if (!normal.isEmpty() || (!bulk.isEmpty() && !shedding)) {
      schedule(TICK_MS);
    } else if (!bulk.isEmpty()) {
      schedule(SHED_RECHECK_MS);
    }
  }

  /** Visits up to {@code max} recipients from the job's position; returns how many were visited. */
  private int write(Job job, int max, ReferenceOpenHashSet<Channel> written) {
    int visited = 0;
    while (visited < max && job.next < job.recipients.size()) {
      Session s = job.recipients.get(job.next++);
      visited++;
      // Unwritable recipients are skipped (spec §6.4): RELIABLE broadcasts carry topic_seq, the SDK resyncs.
      if ((s.state() == SessionState.ACTIVE || s.state() == SessionState.DRAINING) && s.channel().isWritable()) {
        Channel ch = s.channel();
        ch.write(job.frame.retainedDuplicate(), ch.voidPromise());
        written.add(ch);
        job.counter.increment();
      }
    }
    return visited;
  }

  /** Any thread. */
  public int queued() {
    return queued;
  }

  boolean scheduled() {
    return scheduled;
  }
}
