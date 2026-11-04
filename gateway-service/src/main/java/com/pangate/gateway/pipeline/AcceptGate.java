package com.pangate.gateway.pipeline;

import com.pangate.gateway.config.LimitsHolder;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.session.SessionTracker;
import com.pangate.gateway.shed.AdmissionController;
import com.pangate.gateway.shed.IpConnectionCounter;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * First handler, before TLS (spec §4.1, §7.1): drops a connection as cheaply as possible when the node is draining,
 * the loop is at its share of the hard cap, the client IP is at its limit, or the admission bucket is empty.
 */
@Sharable
public final class AcceptGate extends ChannelInboundHandlerAdapter {

  private final NodeState node;
  private final LoopRegistry loops;
  private final LimitsHolder limits;
  private final IpConnectionCounter ips;
  private final AdmissionController admission;
  private final SessionTracker tracker;
  private final GatewayMetrics metrics;
  private final boolean rejectWhenShedding;

  public AcceptGate(NodeState node, LoopRegistry loops, LimitsHolder limits, IpConnectionCounter ips,
      AdmissionController admission, SessionTracker tracker, GatewayMetrics metrics) {
    this(node, loops, limits, ips, admission, tracker, metrics, false);
  }

  private AcceptGate(NodeState node, LoopRegistry loops, LimitsHolder limits, IpConnectionCounter ips,
      AdmissionController admission, SessionTracker tracker, GatewayMetrics metrics, boolean rejectWhenShedding) {
    this.rejectWhenShedding = rejectWhenShedding;
    this.node = node;
    this.loops = loops;
    this.limits = limits;
    this.ips = ips;
    this.admission = admission;
    this.tracker = tracker;
    this.metrics = metrics;
  }

  /**
   * The TCP listener's gate: TCP has no HTTP upgrade to 503, so shed level ≥ 3 (spec §7.3) is enforced here, before
   * TLS, as the equivalent early reject. WS keeps it in {@link UpgradeGuard} (503 + Retry-After).
   */
  public AcceptGate forTcp() {
    return new AcceptGate(node, loops, limits, ips, admission, tracker, metrics, true);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    LoopContext loop = loops.of(ctx.channel().eventLoop());
    InetAddress remote = remote(ctx);
    int perLoopCap = (limits.current().hardCap() + loops.size() - 1) / loops.size();
    String reject = null;
    if (node.draining()) {
      reject = "draining";
    } else if (rejectWhenShedding && node.shedLevel() >= 3) {
      reject = "shed";
    } else if (loop.activeCount() >= perLoopCap) {
      reject = "hard_cap";
    } else if (!ips.tryIncrement(remote, limits.current().perIp())) {
      reject = "per_ip";
    } else if (!admission.tryAdmit(loop.index())) {
      ips.decrement(remote);
      reject = "rate";
    }
    if (reject != null) {
      metrics.admissionRejected(reject);
      ctx.close();
      return;
    }
    tracker.admit(ctx.channel(), loop, remote);
    ctx.pipeline().remove(this);
    ctx.fireChannelActive();
  }

  private static InetAddress remote(ChannelHandlerContext ctx) {
    return ctx.channel().remoteAddress() instanceof InetSocketAddress isa
        ? isa.getAddress()
        : InetAddress.getLoopbackAddress();
  }
}
