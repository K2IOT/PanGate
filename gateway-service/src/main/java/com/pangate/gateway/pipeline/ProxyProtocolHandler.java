package com.pangate.gateway.pipeline;

import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.net.CidrSet;
import com.pangate.gateway.protocol.ChannelAttrs;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.util.ReferenceCountUtil;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * First handler when PROXY protocol is on (spec §4.1): a peer in the trusted CIDRs must send a PROXY v1/v2 header,
 * and {@code channelActive} is held back until it arrives so that {@link AcceptGate} admits the real client address.
 * Other peers, and loopback peers unless {@code trustLoopback}, are direct connections and pass through untouched.
 * One instance per channel.
 */
public final class ProxyProtocolHandler extends ChannelInboundHandlerAdapter {

  private static final String DECODER = "proxyDecoder";

  private final CidrSet trusted;
  private final boolean trustLoopback;
  private final Duration headerTimeout;
  private final GatewayMetrics metrics;
  private ScheduledFuture<?> timeout;
  private boolean done;

  public ProxyProtocolHandler(CidrSet trusted, boolean trustLoopback, Duration headerTimeout, GatewayMetrics metrics) {
    this.trusted = trusted;
    this.trustLoopback = trustLoopback;
    this.headerTimeout = headerTimeout;
    this.metrics = metrics;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    InetAddress peer = ChannelAttrs.socketPeer(ctx.channel());
    if ((peer.isLoopbackAddress() && !trustLoopback) || !trusted.contains(peer)) {
      ctx.pipeline().remove(this);
      ctx.fireChannelActive();
      return;
    }
    // Before this handler, so the decoder's output and its errors (both inbound) reach us.
    ctx.pipeline().addBefore(ctx.name(), DECODER, new HAProxyMessageDecoder());
    timeout = ctx.executor().schedule(() -> {
      if (!done) {
        metrics.admissionRejected("proxy");
        ctx.close();
      }
    }, headerTimeout.toMillis(), TimeUnit.MILLISECONDS);
    ctx.read();  // autoRead may be off while the gateway is starting
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (!(msg instanceof HAProxyMessage header)) {
      if (done) {
        ctx.fireChannelRead(msg);  // bytes the decoder had buffered behind the header
      } else {
        ReferenceCountUtil.release(msg);
        reject(ctx);
      }
      return;
    }
    try {
      if (header.command() == HAProxyCommand.PROXY
          && (header.proxiedProtocol() == HAProxyProxiedProtocol.TCP4
              || header.proxiedProtocol() == HAProxyProxiedProtocol.TCP6)) {
        ctx.channel().attr(ChannelAttrs.CLIENT_ADDRESS)
            .set(new InetSocketAddress(InetAddress.getByName(header.sourceAddress()), header.sourcePort()));
      }
    } catch (java.net.UnknownHostException e) {
      reject(ctx);
      return;
    } finally {
      header.release();
    }
    done = true;
    timeout.cancel(false);
    ctx.fireChannelActive();
    // The decoder removes itself after the header and then forwards what it buffered behind it, through us.
    ctx.pipeline().remove(this);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (!done) {
      reject(ctx);  // not a PROXY header (e.g. a TLS ClientHello from a trusted LB that is not configured for it)
      return;
    }
    ctx.fireExceptionCaught(cause);
  }

  private void reject(ChannelHandlerContext ctx) {
    if (done) {
      return;
    }
    done = true;
    if (timeout != null) {
      timeout.cancel(false);
    }
    metrics.admissionRejected("proxy");
    ctx.close();
  }
}
