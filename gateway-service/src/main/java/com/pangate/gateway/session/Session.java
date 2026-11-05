package com.pangate.gateway.session;

import com.pangate.gateway.config.GatewayLimits;
import com.pangate.gateway.config.LimitsHolder;
import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.loop.Deadlined;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.WheelNode;
import com.pangate.gateway.protocol.ChannelCloser;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.protocol.Frames;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;

/**
 * One client connection (spec §5.1). Flat fields, mutated only on the owning event loop. All deadlines (handshake,
 * HELLO, heartbeat, expiry) go through one wheel entry: {@link #nextDeadlineSec()} is recomputed lazily.
 */
public final class Session extends WheelNode implements Deadlined {

  public static final AttributeKey<Session> KEY = AttributeKey.valueOf("pangate.session");

  /** -Dpangate.assertLoop=true (on in tests): fail fast when a session is mutated off its owning event loop. */
  private static final boolean ASSERT_LOOP = Boolean.getBoolean("pangate.assertLoop");

  /** Who the session belongs to, fixed at HELLO. */
  public record Identity(String tenantId, String principalId, String deviceId, String platform, String sdkVersion,
      Set<String> capabilities) {

    /** Low-cardinality fields are canonicalized and capabilities copied immutably to keep idle sessions small. */
    public Identity {
      tenantId = Canon.of(tenantId);
      platform = Canon.of(platform);
      sdkVersion = Canon.of(sdkVersion);
      capabilities = Set.copyOf(capabilities);
    }
  }

  private final LoopContext loop;
  private final Channel channel;
  private final long sessionKey;
  private final LimitsHolder limits;
  private final TenantLimitsStore tenants;
  private final InetAddress remoteAddress;
  private final long acceptedAtSec;
  private SessionState state = SessionState.HANDSHAKING;
  private long helloDeadlineSec = Long.MAX_VALUE;
  private String connId;
  private Identity identity;
  private String origin;
  private long connectedAtMs;
  private long authAtSec;
  private long expAtSec;
  private long expMonoSec = Long.MAX_VALUE;
  private long authJtiHash;
  private List<String> topicPatterns = List.of();
  private long lastSeenSec;
  private long drainDeadlineSec = Long.MAX_VALUE;
  private CloseCode closeCode;
  // Inbound rate limit state (SessionRateLimiter); tokens < 0 = not initialised yet.
  float tokens = -1;
  long lastRefillNanos;
  int strikes;
  long lastStrikeSec = Long.MIN_VALUE;
  // Upstream bytes in the current 10 s window (Kafka buffer pressure, spec §5.5 step 7).
  private long upstreamBytes;
  private long upstreamWindowSec;
  private boolean upstreamWindowStarted;
  private boolean upstreamPaused;
  // The last registry register sent for this session (RegistryWriter); null once known to be done.
  private java.util.concurrent.CompletableFuture<?> registryWrite;
  private String closeReason;
  // Downstream pushes awaiting ACK (spec §5.4); null while none, so idle sessions stay small.
  private PendingAckWindow pending;
  // In the loop's dirty list (written since the last outbound sample, spec §7.2).
  private boolean dirty;
  // Counted in the node's tenant connection count (spec §4.6 step i); released on close.
  private boolean tenantReserved;
  // Kill switch trace.principal is on for this session's principal (spec §7.8).
  private boolean traced;
  // Since when the channel is unwritable (spec §7.4); MAX while writable.
  private long unwritableSinceSec = Long.MAX_VALUE;
  private long recycleAtSec = Long.MAX_VALUE;
  // Subscribed topics (TopicIndex mirror); null while none.
  private String[] topics;
  private int overflowNacks;
  private long overflowWindowSec;

  public Session(LoopContext loop, Channel channel, long sessionKey, long acceptedAtSec, LimitsHolder limits,
      InetAddress remoteAddress, TenantLimitsStore tenants) {
    this.loop = loop;
    this.channel = channel;
    this.sessionKey = sessionKey;
    this.acceptedAtSec = acceptedAtSec;
    this.limits = limits;
    this.tenants = tenants;
    this.remoteAddress = remoteAddress;
    this.lastSeenSec = acceptedAtSec;
  }

  /** WS upgrade (or TLS + ALPN on TCP) completed: HELLO must arrive within helloTimeout. */
  public void markUpgraded(long nowSec) {
    checkLoop();
    helloDeadlineSec = nowSec + limits.current().helloTimeout().toSeconds();
    reschedule();
  }

  /** {@code authAtSec}/{@code expAtSec} are epoch seconds; expiry is tracked on the monotonic clock. */
  public void activate(Identity id, String connId, long connectedAtMs, long authAtSec, long expAtSec,
      long authJtiHash, List<String> topics) {
    checkLoop();
    this.identity = id;
    this.connId = connId;
    this.connectedAtMs = connectedAtMs;
    this.authAtSec = authAtSec;
    setExpiry(expAtSec);
    this.authJtiHash = authJtiHash;
    this.topicPatterns = List.copyOf(topics);
    this.lastSeenSec = loop.nowSec();
    if (state == SessionState.HANDSHAKING) {
      loop.addHandshaking(-1);
    }
    this.state = SessionState.ACTIVE;
    this.recycleAtSec = loop.recycler() == null || warmUp() ? Long.MAX_VALUE
        : loop.recycler().firstRecycleAtSec(loop.nowSec());
    reschedule();
  }

  public void reauth(long authAtSec, long expAtSec, long jtiHash, List<String> topics) {
    checkLoop();
    this.authAtSec = authAtSec;
    setExpiry(expAtSec);
    this.authJtiHash = jtiHash;
    this.topicPatterns = List.copyOf(topics);
    reschedule();
  }

  private void setExpiry(long expAtWallSec) {
    this.expAtSec = expAtWallSec;
    this.expMonoSec = loop.nowSec() + (expAtWallSec - loop.wallSec());
  }

  /** Any inbound frame: a plain field write, the wheel catches up lazily. */
  public void touch(long nowSec) {
    checkLoop();
    lastSeenSec = nowSec;
  }

  public long nextDeadlineSec() {
    GatewayLimits l = limits.current();
    return switch (state) {
      case HANDSHAKING -> Math.min(acceptedAtSec + l.handshakeTimeout().toSeconds(), helloDeadlineSec);
      // Reads paused for Kafka back-pressure: PINGs cannot arrive, so no heartbeat timeout meanwhile (spec §0.2).
      case ACTIVE -> Math.min(Math.min(Math.min(pendingDeadlineSec(l), slowConsumerDeadlineSec(l)), recycleAtSec),
          upstreamPaused ? expMonoSec : Math.min(lastSeenSec + idleTimeoutSec(), expMonoSec));
      case DRAINING -> Math.min(Math.min(pendingDeadlineSec(l), slowConsumerDeadlineSec(l)),
          Math.min(Math.min(lastSeenSec + idleTimeoutSec(), expMonoSec), drainDeadlineSec));
      case CLOSED -> Long.MAX_VALUE;
    };
  }

  @Override
  public void onDeadline(long nowSec) {
    checkLoop();
    if (state == SessionState.CLOSED) {
      return;
    }
    expirePending(nowSec);
    if (state != SessionState.HANDSHAKING && nowSec >= slowConsumerDeadlineSec(limits.current())) {
      close(CloseCode.SLOW_CONSUMER, "SLOW_CONSUMER");
      return;
    }
    if (state == SessionState.ACTIVE && nowSec >= recycleAtSec) {
      recycleAtSec = loop.recycler().onDue(this, nowSec);
      if (state == SessionState.ACTIVE) {
        reschedule();
        return;
      }
    }
    if (nowSec >= nextDeadlineSec()) {
      if (state == SessionState.HANDSHAKING) {
        close(CloseCode.HELLO_TIMEOUT, "HELLO_TIMEOUT");
      } else if (state == SessionState.DRAINING && nowSec >= drainDeadlineSec) {
        close(CloseCode.GOING_AWAY, "DRAINED");
      } else if (nowSec >= expMonoSec) {
        close(CloseCode.SESSION_EXPIRED, "SESSION_EXPIRED");
      } else {
        close(CloseCode.GOING_AWAY, "HEARTBEAT_TIMEOUT");
      }
      return;
    }
    reschedule();
  }

  private long slowConsumerDeadlineSec(GatewayLimits l) {
    return unwritableSinceSec == Long.MAX_VALUE ? Long.MAX_VALUE
        : unwritableSinceSec + l.slowConsumerTimeout().toSeconds();
  }

  /** Netty writability changed: unwritable starts the slow-consumer deadline, writable clears it. */
  public void writabilityChanged(boolean writable, long nowSec) {
    checkLoop();
    if (state == SessionState.CLOSED) {
      return;
    }
    if (!writable && unwritableSinceSec == Long.MAX_VALUE) {
      unwritableSinceSec = nowSec;
      loop.addUnwritable(1);
    } else if (writable && unwritableSinceSec != Long.MAX_VALUE) {
      unwritableSinceSec = Long.MAX_VALUE;
      loop.addUnwritable(-1);
    }
    reschedule();
  }

  public boolean unwritable() {
    return unwritableSinceSec != Long.MAX_VALUE;
  }

  private long pendingDeadlineSec(GatewayLimits l) {
    return pending == null ? Long.MAX_VALUE : pending.oldestSentAtSec() + l.pendingAckTimeout().toSeconds();
  }

  /** Overdue pushes are NACK_TIMEOUT and forgotten; never re-sent on this connection (spec §5.4). */
  private void expirePending(long nowSec) {
    if (pending != null) {
      int before = pending.size();
      pending.expire(nowSec, limits.current().pendingAckTimeout().toSeconds(),
          e -> {
            e.answer(com.pangate.contracts.v1.AckItem.Status.STATUS_NACK_TIMEOUT);
            com.pangate.gateway.trace.GatewayTracing.current().nackPending(this, e.msgId(),
                com.pangate.contracts.v1.AckItem.Status.STATUS_NACK_TIMEOUT);
          });
      loop.addPendingInflight(pending.size() - before);
      releasePendingIfEmpty();
    }
  }

  public String[] topics() {
    return topics;
  }

  public boolean hasTopic(String topic) {
    if (topics != null) {
      for (String t : topics) {
        if (t.equals(topic)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Only through {@link com.pangate.gateway.downstream.TopicIndex}, which keeps the loop's index in step. */
  public boolean addTopic(String topic) {
    checkLoop();
    if (hasTopic(topic)) {
      return false;
    }
    if (topics == null) {
      topics = new String[] {topic};
    } else {
      topics = java.util.Arrays.copyOf(topics, topics.length + 1);
      topics[topics.length - 1] = topic;
    }
    return true;
  }

  public boolean removeTopic(String topic) {
    checkLoop();
    if (!hasTopic(topic)) {
      return false;
    }
    if (topics.length == 1) {
      topics = null;
      return true;
    }
    String[] left = new String[topics.length - 1];
    int i = 0;
    for (String t : topics) {
      if (!t.equals(topic)) {
        left[i++] = t;
      }
    }
    topics = left;
    return true;
  }

  public boolean dirty() {
    return dirty;
  }

  public void dirty(boolean value) {
    dirty = value;
  }

  /** Bytes queued in Netty's outbound buffer for this connection (0 once closed). */
  public long pendingOutboundBytes() {
    io.netty.channel.ChannelOutboundBuffer buf = channel.unsafe().outboundBuffer();
    return buf == null ? 0 : buf.totalPendingWriteBytes();
  }

  public PendingAckWindow pending() {
    return pending;
  }

  public PendingAckWindow pendingOrCreate(GatewayLimits l) {
    checkLoop();
    if (pending == null) {
      pending = new PendingAckWindow(l.pendingAckMsgs(), l.pendingAckBytes());
    }
    return pending;
  }

  public void releasePendingIfEmpty() {
    if (pending != null && pending.size() == 0) {
      pending = null;
    }
  }

  /** Adds to the window (created if needed); the first entry puts the ack deadline on the wheel. */
  public void addPending(PendingEntry e) {
    checkLoop();
    boolean first = pending == null || pending.size() == 0;
    pendingOrCreate(limits.current()).add(e);
    loop.addPendingInflight(1);
    if (first) {
      reschedule();
    }
  }

  /** The client acknowledged {@code msgId}: the entry, or null if unknown (already acked or timed out). */
  public PendingEntry ackPending(String msgId) {
    checkLoop();
    if (pending == null) {
      return null;
    }
    PendingEntry e = pending.remove(msgId);
    if (e != null) {
      loop.addPendingInflight(-1);
      releasePendingIfEmpty();
    }
    return e;
  }

  /** Every remaining entry is answered with {@code status} (close: NACK_CLOSED or NACK_SLOW_CONSUMER). */
  public void failPending(com.pangate.contracts.v1.AckItem.Status status) {
    if (pending != null) {
      loop.addPendingInflight(-pending.size());
      pending.drain(e -> {
        e.answer(status);
        com.pangate.gateway.trace.GatewayTracing.current().nackPending(this, e.msgId(), status);
      });
      pending = null;
    }
  }

  /** One NACK_OVERFLOW; true once 64 happened within the current 30 s window (spec §5.3, §7.4). */
  public boolean countOverflowNack(long nowSec) {
    if (overflowNacks == 0 || nowSec - overflowWindowSec >= 30) {
      overflowWindowSec = nowSec;
      overflowNacks = 0;
    }
    return ++overflowNacks >= 64;
  }

  /** Recomputes the wheel slot now (a kill switch shortened the heartbeat; lazy rebucketing would be too late). */
  public void refreshDeadline() {
    checkLoop();
    reschedule();
  }

  /** The tenant's effective heartbeat interval (kill switch heartbeat.intervalSec applies at once). */
  public long heartbeatSec() {
    return tenants.policy(identity == null ? null : identity.tenantId()).heartbeatSec();
  }

  /** Heartbeat timeout of the tenant's effective policy (kill switch heartbeat.intervalSec applies at once). */
  private long idleTimeoutSec() {
    return tenants.policy(identity == null ? null : identity.tenantId()).idleTimeoutSec();
  }

  /**
   * Tells the client to reconnect after {@code delayMs} (spec §5.9, §6.8 cell migration) and closes it with 1001
   * {@code delay + 30 s} later if it has not. Only from ACTIVE.
   */
  public void drain(long delayMs, boolean reBootstrap) {
    drain(delayMs, reBootstrap, 30);
  }

  /** As {@link #drain(long, boolean)}, closing with 1001 {@code delay + graceSec} later. */
  public void drain(long delayMs, boolean reBootstrap, long graceSec) {
    checkLoop();
    if (state != SessionState.ACTIVE) {
      return;
    }
    channel.writeAndFlush(Frames.reconnect(com.pangate.contracts.v1.Reconnect.newBuilder()
        .setDelayMs((int) delayMs).setReBootstrap(reBootstrap).build()));
    state = SessionState.DRAINING;
    drainDeadlineSec = loop.nowSec() + (delayMs + 999) / 1000 + graceSec;
    reschedule();
  }

  /** Idempotent; cleanup of indexes happens when the channel's close future completes (SessionTracker). */
  public void close(CloseCode code, String reason) {
    close(code, reason, code.defaultRetryAfterMs());
  }

  public void close(CloseCode code, String reason, long retryAfterMs) {
    checkLoop();
    if (state == SessionState.CLOSED) {
      return;
    }
    state = SessionState.CLOSED;
    closeCode = code;
    closeReason = reason;
    loop.wheel().remove(this);
    ChannelCloser.close(channel, code, reason, retryAfterMs);
  }

  /** Records how a session ended when the channel closed without {@link #close} (client close, reset). */
  void markClosedBy(CloseCode code) {
    state = SessionState.CLOSED;
    if (closeCode == null) {
      closeCode = code;
    }
  }

  private void checkLoop() {
    if (ASSERT_LOOP && !loop.inLoop()) {
      throw new IllegalStateException("Session " + sessionKey + " mutated off its event loop on "
          + Thread.currentThread().getName());
    }
  }

  void reschedule() {
    if (state != SessionState.CLOSED) {
      loop.wheel().schedule(this, nextDeadlineSec());
    }
  }

  public boolean traced() {
    return traced;
  }

  public void traced(boolean value) {
    traced = value;
  }

  public boolean tenantReserved() {
    return tenantReserved;
  }

  public void tenantReserved(boolean value) {
    tenantReserved = value;
  }

  /** A warm-up session (spec §7.6): no registry, Kafka, recycling or business metrics. */
  public boolean warmUp() {
    return identity != null && com.pangate.gateway.auth.WarmUpTokens.isWarmUp(identity.tenantId());
  }

  public LoopContext loop() {
    return loop;
  }

  public Channel channel() {
    return channel;
  }

  public long sessionKey() {
    return sessionKey;
  }

  public SessionState state() {
    return state;
  }

  public String connId() {
    return connId;
  }

  public Identity identity() {
    return identity;
  }

  /** Wall-clock time of activation (epoch ms); written to the registry and compared by KICK. */
  public long connectedAtMs() {
    return connectedAtMs;
  }

  public long authAtSec() {
    return authAtSec;
  }

  public long expAtSec() {
    return expAtSec;
  }

  public long authJtiHash() {
    return authJtiHash;
  }

  public List<String> topicPatterns() {
    return topicPatterns;
  }

  public long lastSeenSec() {
    return lastSeenSec;
  }

  public void addUpstreamBytes(int n, long nowSec) {
    if (!upstreamWindowStarted || nowSec - upstreamWindowSec >= 10) {
      upstreamWindowStarted = true;
      upstreamWindowSec = nowSec;
      upstreamBytes = 0;
    }
    upstreamBytes += n;
  }

  public long upstreamBytes10s(long nowSec) {
    return !upstreamWindowStarted || nowSec - upstreamWindowSec >= 10 ? 0 : upstreamBytes;
  }

  public boolean upstreamPaused() {
    return upstreamPaused;
  }

  public java.util.concurrent.CompletableFuture<?> registryWrite() {
    return registryWrite;
  }

  public void registryWrite(java.util.concurrent.CompletableFuture<?> write) {
    checkLoop();
    registryWrite = write;
  }

  /** Resuming counts as a sign of life: the heartbeat clock restarts from now. */
  public void upstreamPaused(boolean paused) {
    checkLoop();
    upstreamPaused = paused;
    if (!paused) {
      lastSeenSec = loop.nowSec();
    }
    reschedule();
  }

  /** Consecutive seconds with an upstream rate violation (spec §5.5). */
  public int strikes() {
    return strikes;
  }

  public CloseCode closeCode() {
    return closeCode;
  }

  public String closeReason() {
    return closeReason;
  }

  public InetAddress remoteAddress() {
    return remoteAddress;
  }

  public String origin() {
    return origin;
  }

  public void origin(String origin) {
    this.origin = origin;
  }
}
