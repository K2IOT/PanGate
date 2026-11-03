package com.pangate.gateway.pipeline;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.session.Session;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.util.Arrays;
import java.util.random.RandomGenerator;

/**
 * Checks the WebSocket upgrade request before the handshake (spec §4.1): path, subprotocol pangate.v1, node soft cap
 * and draining. Records Origin for the post-auth check, then forwards the request and removes itself.
 */
public final class UpgradeGuard extends ChannelInboundHandlerAdapter {

  static final String SUBPROTOCOL = "pangate.v1";

  private final GatewayProperties props;
  private final com.pangate.gateway.config.LimitsHolder limits;
  private final LoopRegistry loops;
  private final NodeState node;
  private final RandomGenerator rnd;

  public UpgradeGuard(GatewayProperties props, com.pangate.gateway.config.LimitsHolder limits, LoopRegistry loops,
      NodeState node, RandomGenerator rnd) {
    this.props = props;
    this.limits = limits;
    this.loops = loops;
    this.node = node;
    this.rnd = rnd;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (!(msg instanceof FullHttpRequest req)) {
      ctx.fireChannelRead(msg);
      return;
    }
    if (!props.listeners().ws().path().equals(new QueryStringDecoder(req.uri()).path())) {
      reject(ctx, req, HttpResponseStatus.NOT_FOUND, 0);
      return;
    }
    String protocols = req.headers().get(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, "");
    if (Arrays.stream(protocols.split(",")).map(String::trim).noneMatch(SUBPROTOCOL::equals)) {
      reject(ctx, req, HttpResponseStatus.UPGRADE_REQUIRED, 0);
      return;
    }
    if (node.draining() || loops.totalActive() >= limits.current().softCap()) {
      reject(ctx, req, HttpResponseStatus.SERVICE_UNAVAILABLE, 5 + rnd.nextInt(26));
      return;
    }
    Session s = ctx.channel().attr(Session.KEY).get();
    if (s != null) {
      s.origin(req.headers().get(HttpHeaderNames.ORIGIN));
    }
    ctx.pipeline().remove(this);
    ctx.fireChannelRead(req);
  }

  private static void reject(ChannelHandlerContext ctx, FullHttpRequest req, HttpResponseStatus status,
      int retryAfterSec) {
    req.release();
    FullHttpResponse res = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);
    res.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE).setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
    if (status == HttpResponseStatus.UPGRADE_REQUIRED) {
      res.headers().set(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, SUBPROTOCOL);
    }
    if (retryAfterSec > 0) {
      res.headers().setInt(HttpHeaderNames.RETRY_AFTER, retryAfterSec);
    }
    ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
  }
}
