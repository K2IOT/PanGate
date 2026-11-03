package com.pangate.gateway.test;

import com.pangate.gateway.config.LimitsHolder;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.session.PrincipalIndex;
import com.pangate.gateway.session.SessionTracker;
import com.pangate.gateway.shed.IpConnectionCounter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** One EmbeddedChannel bound to a LoopContext with a fake clock, plus the tracking collaborators. */
public final class LoopFixture {

  /** Monotonic seconds: drives the wheel and session deadlines. */
  public final AtomicLong clockSec = new AtomicLong(1_762_000_000L);
  /** Wall-clock epoch seconds: token and ticket times. Moves with clockSec unless a test makes it jump. */
  public final AtomicLong wallOffset = new AtomicLong();
  public final EmbeddedChannel channel;
  public final LoopContext loop;
  public final LoopRegistry loops;
  public final LimitsHolder limits = TestLimits.holder();
  public final PrincipalIndex principals = new PrincipalIndex();
  public final IpConnectionCounter ips = new IpConnectionCounter();
  public final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  public final GatewayMetrics metrics = new GatewayMetrics(meters);
  public final SessionTracker tracker;

  public LoopFixture() {
    channel = new EmbeddedChannel();
    loop = new LoopContext(0, channel.eventLoop(), 128, clockSec::get, () -> clockSec.get() + wallOffset.get());
    loops = LoopRegistry.fromContexts(List.of(loop));
    tracker = new SessionTracker(loops, principals, ips, limits, metrics, () -> clockSec.get() * 1000);
  }

  /** Moves the clock and runs the wheel as the per-second ticker would. */
  public void advanceTo(long sec) {
    clockSec.set(sec);
    loop.advance(sec);
    channel.runPendingTasks();
  }

  public long now() {
    return clockSec.get();
  }

  public long wallNow() {
    return clockSec.get() + wallOffset.get();
  }
}
