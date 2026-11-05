package com.pangate.gateway.downstream;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Subscribe;
import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.protocol.Frames;
import com.pangate.gateway.protocol.ProtocolException;
import com.pangate.gateway.session.Session;
import io.micrometer.core.instrument.Counter;
import io.netty.channel.ChannelHandlerContext;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Client SUBSCRIBE / UNSUBSCRIBE (spec §5.7), on the owner loop. A frame is applied whole or not at all; success is
 * silent, a rejection answers ERROR 4000 carrying the frame's msg_id and keeps the connection.
 */
public final class ClientTopics {

  private final TenantLimitsStore tenants;
  private final Counter notAllowed;
  private final Counter limit;
  private final Counter invalid;

  public ClientTopics(TenantLimitsStore tenants, GatewayMetrics m) {
    this.tenants = tenants;
    this.notAllowed = m.counter("gw.subscribe.rejected", "reason", "not_allowed");
    this.limit = m.counter("gw.subscribe.rejected", "reason", "limit");
    this.invalid = m.counter("gw.subscribe.rejected", "reason", "invalid");
  }

  public void onFrame(ChannelHandlerContext ctx, Session s, Frame frame) {
    Subscribe req;
    try {
      req = Subscribe.parseFrom(frame.getPayload());
    } catch (InvalidProtocolBufferException e) {
      throw new ProtocolException(CloseCode.BAD_FRAME, "malformed " + frame.getType());
    }
    TopicIndex index = s.loop().topics();
    if (frame.getType() == FrameType.FRAME_TYPE_UNSUBSCRIBE) {
      req.getTopicsList().forEach(t -> index.remove(s, t));
      return;
    }
    Set<String> fresh = new LinkedHashSet<>();
    for (String t : req.getTopicsList()) {
      if (t.length() > TopicPatterns.MAX_TOPIC_LENGTH) {
        invalid.increment();
        reject(ctx, frame, "topic too long");
        return;
      }
      if (!TopicPatterns.inTenant(s.identity().tenantId(), t) || !TopicPatterns.allowed(s.topicPatterns(), t)) {
        notAllowed.increment();
        reject(ctx, frame, "topic not allowed: " + t);
        return;
      }
      if (!s.hasTopic(t)) {
        fresh.add(t);
      }
    }
    int held = s.topics() == null ? 0 : s.topics().length;
    if (held + fresh.size() > tenants.policy(s.identity().tenantId()).topicsPerConn()) {
      limit.increment();
      reject(ctx, frame, "too many topics");
      return;
    }
    fresh.forEach(t -> index.add(s, t));
  }

  /**
   * After REAUTH replaced the session's topic claims: drop the subscriptions the new claims no longer allow, so a
   * narrower JWT also narrows what broadcasts reach the client. Owner loop only.
   */
  public static void pruneDisallowed(Session s) {
    String[] held = s.topics();
    if (held == null) {
      return;
    }
    TopicIndex index = s.loop().topics();
    for (String t : held.clone()) {
      if (!TopicPatterns.allowed(s.topicPatterns(), t)) {
        index.remove(s, t);
      }
    }
  }

  private static void reject(ChannelHandlerContext ctx, Frame frame, String message) {
    ctx.writeAndFlush(Frames.error(CloseCode.BAD_FRAME.code(), message, false, false, 0, frame.getMsgId()));
  }
}
