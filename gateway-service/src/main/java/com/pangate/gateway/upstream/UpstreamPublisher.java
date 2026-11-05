package com.pangate.gateway.upstream;

import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.UpstreamRecord;
import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.control.TenantPolicy;
import com.pangate.gateway.lifecycle.KafkaClients;
import com.pangate.gateway.lifecycle.TopicNames;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.protocol.Frames;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionRateLimiter;
import com.pangate.starter.kafka.PangateHeaders;
import io.micrometer.core.instrument.Counter;
import io.netty.channel.ChannelHandlerContext;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Client UPSTREAM (and RPC_RES as {@code cmd_result}) to Kafka (spec §5.5). Runs on the owning event loop; the
 * producers never block it ({@code max.block.ms=0}). {@code cmd}/{@code cmd_result} are acknowledged to the client
 * once Kafka acknowledged them (or answered UNAVAILABLE); telemetry is fire-and-forget.
 */
public final class UpstreamPublisher {

  private static final String UNKNOWN = "unknown";
  private static final String[] RESULTS = {"ok", "error", "dropped", "rate_limited", "unknown_type"};

  private final KafkaClients kafka;
  private final TopicNames topics;
  private final NodeState node;
  private final String cell;
  private final TenantLimitsStore tenants;
  private final LongSupplier wallMillis;
  private final LongSupplier nanos;
  private final Map<String, Counter[]> counters = new HashMap<>();

  public UpstreamPublisher(KafkaClients kafka, TopicNames topics, NodeState node, String cell,
      TenantLimitsStore tenants, GatewayMetrics metrics, LongSupplier wallMillis, LongSupplier nanos) {
    this.kafka = kafka;
    this.topics = topics;
    this.node = node;
    this.cell = cell;
    this.tenants = tenants;
    this.wallMillis = wallMillis;
    this.nanos = nanos;
    for (String type : topics.upstreamTypes()) {
      counters.put(type, counters(metrics, type));
    }
    counters.put(UNKNOWN, counters(metrics, UNKNOWN));
  }

  private static Counter[] counters(GatewayMetrics metrics, String type) {
    Counter[] c = new Counter[RESULTS.length];
    for (int i = 0; i < RESULTS.length; i++) {
      c[i] = metrics.counter("gw.upstream", "type", type, "result", RESULTS[i]);
    }
    return c;
  }

  private void count(String type, int result) {
    counters.getOrDefault(type, counters.get(UNKNOWN))[result].increment();
  }

  /** @param forcedType {@code cmd_result} for RPC_RES, otherwise null (type comes from the frame headers). */
  public void publish(ChannelHandlerContext ctx, Session s, Frame f, String forcedType) {
    TenantPolicy policy = tenants.policy(s.identity().tenantId());
    String type = forcedType != null ? forcedType : f.getHeadersOrDefault("type", "");
    switch (SessionRateLimiter.check(s, policy, nanos.getAsLong(), s.loop().nowSec())) {
      case LIMITED -> {
        count(type, 3);
        ctx.writeAndFlush(Frames.error(CloseCode.RATE_LIMITED.code(), "RATE_LIMITED", true, false));
        return;
      }
      case CLOSE -> {
        count(type, 3);
        s.close(CloseCode.RATE_LIMITED, "RATE_LIMITED");
        return;
      }
      case OK -> { }
    }
    if (!topics.isUpstreamType(type)) {
      count(UNKNOWN, 4);
      ctx.writeAndFlush(Frames.error(CloseCode.BAD_FRAME.code(), "unknown type", false, false));
      return;
    }
    if (f.getMsgId().isEmpty()) {
      count(type, 4);
      ctx.writeAndFlush(Frames.error(CloseCode.BAD_FRAME.code(), "missing msg_id", false, false));
      return;
    }
    boolean reliable = TopicNames.isReliable(type);
    // Telemetry is dropped by kill switch, or by the shed ladder from level 2 (spec §5.5 step 3, §7.3).
    if (!reliable && (!policy.telemetryEnabled() || node.shedLevel() >= 2)) {
      count(type, 2);
      return;
    }
    s.addUpstreamBytes(f.getPayload().size(), s.loop().nowSec());
    Session.Identity id = s.identity();
    ProducerRecord<String, byte[]> r = record(f, type, id.tenantId(), id.principalId(), id.deviceId(), s.connId());
    Producer<String, byte[]> producer = reliable ? kafka.reliable() : kafka.telemetry();
    String msgId = f.getMsgId();
    try {
      producer.send(r, (md, ex) -> {
        count(type, ex == null ? 0 : 1);
        if (reliable) {
          s.loop().eventLoop().execute(() -> answer(ctx, msgId, ex == null));
        }
      });
    } catch (RuntimeException ex) {
      count(type, 1);
      if (reliable) {
        answer(ctx, msgId, false);
      }
    }
  }

  private ProducerRecord<String, byte[]> record(Frame f, String type, String tenant, String principal, String device,
      String connId) {
    UpstreamRecord.Builder rec = UpstreamRecord.newBuilder()
        .setMsgId(f.getMsgId())
        .setTenantId(tenant)
        .setPrincipalId(principal)
        .setDeviceId(device)
        .setConnId(connId)
        .setNodeId(node.nodeId())
        .setCellId(cell)
        .setReceivedAtMs(wallMillis.getAsLong())
        .putAllHeaders(f.getHeadersMap())
        .setPayload(f.getPayload());
    rec.removeHeaders("type");
    ProducerRecord<String, byte[]> r = new ProducerRecord<>(topics.upstream(type), principal, rec.build().toByteArray());
    PangateHeaders.write(r.headers(), f.getHeadersOrDefault("traceparent", null), tenant, cell, "UpstreamRecord");
    return r;
  }

  /** Outcome of {@code POST /v1/upstream} (spec §4.3), mapped to an HTTP status by the handler. */
  public enum HttpOutcome { ACCEPTED, UNKNOWN_TYPE, MISSING_MSG_ID, UNAVAILABLE }

  /**
   * UPSTREAM over HTTP with a resume ticket (spec §4.3): same type, telemetry and shed rules as {@link #publish}, no
   * session ({@code conn_id} empty, no per-session rate limit). Telemetry that is dropped still answers ACCEPTED;
   * {@code cmd}/{@code cmd_result} complete when Kafka acknowledged them. Completes on a Kafka thread.
   */
  public java.util.concurrent.CompletionStage<HttpOutcome> publishHttp(com.pangate.gateway.auth.ResumeTicket t,
      Frame f) {
    String type = f.getHeadersOrDefault("type", "");
    if (!topics.isUpstreamType(type)) {
      count(UNKNOWN, 4);
      return java.util.concurrent.CompletableFuture.completedFuture(HttpOutcome.UNKNOWN_TYPE);
    }
    if (f.getMsgId().isEmpty()) {
      count(type, 4);
      return java.util.concurrent.CompletableFuture.completedFuture(HttpOutcome.MISSING_MSG_ID);
    }
    boolean reliable = TopicNames.isReliable(type);
    if (!reliable && (!tenants.policy(t.tenant()).telemetryEnabled() || node.shedLevel() >= 2)) {
      count(type, 2);
      return java.util.concurrent.CompletableFuture.completedFuture(HttpOutcome.ACCEPTED);
    }
    ProducerRecord<String, byte[]> r = record(f, type, t.tenant(), t.principal(), t.device(), "");
    java.util.concurrent.CompletableFuture<HttpOutcome> done = new java.util.concurrent.CompletableFuture<>();
    try {
      (reliable ? kafka.reliable() : kafka.telemetry()).send(r, (md, ex) -> {
        count(type, ex == null ? 0 : 1);
        if (reliable) {
          done.complete(ex == null ? HttpOutcome.ACCEPTED : HttpOutcome.UNAVAILABLE);
        }
      });
    } catch (RuntimeException ex) {
      count(type, 1);
      return java.util.concurrent.CompletableFuture.completedFuture(reliable ? HttpOutcome.UNAVAILABLE
          : HttpOutcome.ACCEPTED);
    }
    if (!reliable) {
      done.complete(HttpOutcome.ACCEPTED);
    }
    return done;
  }

  private static void answer(ChannelHandlerContext ctx, String msgId, boolean ok) {
    if (!ctx.channel().isActive()) {
      return;
    }
    ctx.writeAndFlush(ok ? Frames.upstreamAck(msgId)
        : Frames.error(CloseCode.INTERNAL.code(), "UNAVAILABLE", true, false));
  }
}
