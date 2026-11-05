package com.pangate.gateway.upstream;

import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import io.micrometer.core.instrument.Counter;
import java.util.PriorityQueue;
import java.util.function.DoubleSupplier;

/**
 * Kafka buffer back-pressure (spec §5.5 step 7). Sampled every 100 ms on gw-tick: when a producer's buffer is over
 * 90% full each loop stops reading from its noisiest 1% of sessions (by upstream bytes in the last 10 s, at least one);
 * below 50% every paused session reads again. Selection and autoRead changes run on the owning loop.
 */
public final class UpstreamPressure {

  static final double PAUSE_ABOVE = 0.9;
  static final double RESUME_BELOW = 0.5;

  private final DoubleSupplier maxBufferRatio;
  private final LoopRegistry loops;
  private final Counter pausedSessions;
  private volatile double lastRatio;
  private volatile boolean paused;

  public UpstreamPressure(DoubleSupplier maxBufferRatio, LoopRegistry loops, GatewayMetrics metrics) {
    this.maxBufferRatio = maxBufferRatio;
    this.loops = loops;
    this.pausedSessions = metrics.counter("gw.upstream.paused.sessions");
    metrics.gauge("gw.kafka.buffer.ratio", this, p -> p.lastRatio);
  }

  public void sample() {
    double r = maxBufferRatio.getAsDouble();
    lastRatio = r;
    if (!paused && r > PAUSE_ABOVE) {
      paused = true;
      loops.all().forEach(c -> c.eventLoop().execute(() -> pauseNoisiest(c)));
    } else if (paused && r < RESUME_BELOW) {
      paused = false;
      loops.all().forEach(c -> c.eventLoop().execute(() -> resumeAll(c)));
    }
  }

  private void pauseNoisiest(LoopContext c) {
    long now = c.nowSec();
    int n = Math.max(1, (int) Math.ceil(c.sessionCount() / 100.0));
    PriorityQueue<Session> top = new PriorityQueue<>(n + 1,
        (a, b) -> Long.compare(a.upstreamBytes10s(now), b.upstreamBytes10s(now)));
    c.forEachSession(s -> {
      if (s.state() == SessionState.ACTIVE && s.upstreamBytes10s(now) > 0) {
        top.add(s);
        if (top.size() > n) {
          top.poll();
        }
      }
    });
    for (Session s : top) {
      s.upstreamPaused(true);
      s.channel().config().setAutoRead(false);
      pausedSessions.increment();
    }
  }

  private static void resumeAll(LoopContext c) {
    c.forEachSession(s -> {
      if (s.upstreamPaused()) {
        s.upstreamPaused(false);
        if (s.channel().isActive()) {
          s.channel().config().setAutoRead(true);
        }
      }
    });
  }

  public boolean paused() {
    return paused;
  }
}
