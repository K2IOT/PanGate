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

  public AcceptGate(NodeState node, LoopRegistry loops, LimitsHolder limits, IpConnectionCounter ips,
      AdmissionController admission, SessionTracker tracker, GatewayMetrics metrics) {
    this.node = node;
    this.loops = loops;
    this.limits = limits;
    this.ips = ips;
    this.admission = admission;
    this.tracker = tracker;
    this.metrics = metrics;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    LoopContext loop = loops.of(ctx.channel().eventLoop());
    InetAddress remote = remote(ctx);
    int perLoopCap = (limits.current().hardCap() + loops.size() - 1) / loops.size();
    String reject = null;
    if (node.draining()) {
      reject = "draining";
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
