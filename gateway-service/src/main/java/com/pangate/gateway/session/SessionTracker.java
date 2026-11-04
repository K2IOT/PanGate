package com.pangate.gateway.session;

import com.pangate.gateway.config.LimitsHolder;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.shed.IpConnectionCounter;
import io.netty.channel.Channel;
import java.net.InetAddress;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

/**
 * Single owner of session bookkeeping: admission into the loop, activation into the principal index, and the one
 * cleanup path that runs when the channel's close future completes (whatever closed it).
 */
public final class SessionTracker {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SessionTracker.class);

  private final LoopRegistry loops;
  private final PrincipalIndex principals;
  private final IpConnectionCounter ips;
  private final LimitsHolder limits;
  private final GatewayMetrics metrics;
  private final LongSupplier epochMillis;
  private final java.util.List<SessionListener> listeners;
  private final com.pangate.gateway.control.TenantLimitsStore tenants;

  public SessionTracker(LoopRegistry loops, PrincipalIndex principals, IpConnectionCounter ips, LimitsHolder limits,
      GatewayMetrics metrics, LongSupplier epochMillis, java.util.List<SessionListener> listeners,
      com.pangate.gateway.control.TenantLimitsStore tenants) {
    this.loops = loops;
    this.principals = principals;
    this.ips = ips;
    this.limits = limits;
    this.metrics = metrics;
    this.epochMillis = epochMillis;
    this.listeners = listeners;
    this.tenants = tenants;
  }

  /** Called on the channel's loop after admission (the IP counter is already incremented). */
  public Session admit(Channel ch, LoopContext loop, InetAddress remote) {
    Session s = new Session(loop, ch, loop.nextSessionKey(), loop.nowSec(), limits, remote, tenants);
    ch.attr(Session.KEY).set(s);
    loop.register(s);
    loop.incrementActive();
    s.reschedule();
    ch.closeFuture().addListener(f -> onClosed(s));
    return s;
  }

  public void onActivated(Session s) {
    principals.add(s.identity().principalId(), new ConnRef(s.loop().index(), s.sessionKey()));
    for (SessionListener l : listeners) {
      try {
        l.activated(s);
      } catch (RuntimeException e) {
        log.warn("session listener {} failed on activation: {}", l.getClass().getSimpleName(), e.toString());
      }
    }
  }

  public String newConnId(Session s) {
    return ConnId.next(epochMillis.getAsLong(), s.loop().index(), s.sessionKey(), ThreadLocalRandom.current());
  }

  void onClosed(Session s) {
    LoopContext loop = s.loop();
    Integer clientCode = s.channel().attr(ChannelAttrs.CLIENT_CLOSE_CODE).get();
    s.markClosedBy(clientCode != null ? CloseCode.of(clientCode) : CloseCode.ABNORMAL);
    loop.wheel().remove(s);
    s.failPending(s.closeCode() == CloseCode.SLOW_CONSUMER
        ? com.pangate.contracts.v1.AckItem.Status.STATUS_NACK_SLOW_CONSUMER
        : com.pangate.contracts.v1.AckItem.Status.STATUS_NACK_CLOSED);
    loop.unregister(s);
    loop.decrementActive();
    if (s.identity() != null) {
      principals.remove(s.identity().principalId(), new ConnRef(loop.index(), s.sessionKey()));
    }
    ips.decrement(s.remoteAddress());
    metrics.closed(s.closeCode());
    if (s.identity() != null) {
      for (SessionListener l : listeners) {
        try {
          l.closed(s);
        } catch (RuntimeException e) {
          log.warn("session listener {} failed on close: {}", l.getClass().getSimpleName(), e.toString());
        }
      }
    }
  }

  public LoopRegistry loops() {
    return loops;
  }
}
