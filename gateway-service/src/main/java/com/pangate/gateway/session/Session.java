package com.pangate.gateway.session;

import com.pangate.gateway.config.GatewayLimits;
import com.pangate.gateway.config.LimitsHolder;
import com.pangate.gateway.loop.Deadlined;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.WheelNode;
import com.pangate.gateway.protocol.ChannelCloser;
import com.pangate.gateway.protocol.CloseCode;
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
      Set<String> capabilities) {}

  private final LoopContext loop;
  private final Channel channel;
  private final long sessionKey;
  private final LimitsHolder limits;
  private final InetAddress remoteAddress;
  private final long acceptedAtSec;
  private SessionState state = SessionState.HANDSHAKING;
  private long helloDeadlineSec = Long.MAX_VALUE;
  private String connId;
  private Identity identity;
  private String origin;
  private long authAtSec;
  private long expAtSec;
  private long expMonoSec = Long.MAX_VALUE;
  private long authJtiHash;
  private List<String> topicPatterns = List.of();
  private long lastSeenSec;
  private CloseCode closeCode;
  private String closeReason;

  public Session(LoopContext loop, Channel channel, long sessionKey, long acceptedAtSec, LimitsHolder limits,
      InetAddress remoteAddress) {
    this.loop = loop;
    this.channel = channel;
    this.sessionKey = sessionKey;
    this.acceptedAtSec = acceptedAtSec;
    this.limits = limits;
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
  public void activate(Identity id, String connId, long authAtSec, long expAtSec, long authJtiHash,
      List<String> topics) {
    checkLoop();
    this.identity = id;
    this.connId = connId;
    this.authAtSec = authAtSec;
    setExpiry(expAtSec);
    this.authJtiHash = authJtiHash;
    this.topicPatterns = List.copyOf(topics);
    this.lastSeenSec = loop.nowSec();
    this.state = SessionState.ACTIVE;
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
      case ACTIVE -> Math.min(lastSeenSec + l.idleTimeoutSec(), expMonoSec);
      case CLOSED -> Long.MAX_VALUE;
    };
  }

  @Override
  public void onDeadline(long nowSec) {
    checkLoop();
    if (state == SessionState.CLOSED) {
      return;
    }
    if (nowSec >= nextDeadlineSec()) {
      if (state == SessionState.HANDSHAKING) {
        close(CloseCode.HELLO_TIMEOUT, "HELLO_TIMEOUT");
      } else if (nowSec >= expMonoSec) {
        close(CloseCode.SESSION_EXPIRED, "SESSION_EXPIRED");
      } else {
        close(CloseCode.GOING_AWAY, "HEARTBEAT_TIMEOUT");
      }
      return;
    }
    reschedule();
  }

  /** Idempotent; cleanup of indexes happens when the channel's close future completes (SessionTracker). */
  public void close(CloseCode code, String reason) {
    checkLoop();
    if (state == SessionState.CLOSED) {
      return;
    }
    state = SessionState.CLOSED;
    closeCode = code;
    closeReason = reason;
    loop.wheel().remove(this);
    ChannelCloser.close(channel, code, reason, code.defaultRetryAfterMs());
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
