package com.pangate.gateway.trace;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.PushItem;
import com.pangate.gateway.session.Session;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Push spans (spec §8): {@code gw.push.handoff} (router enqueue → gateway write) and {@code gw.push.write} (write →
 * flushed), only for a sampled {@code traceparent} or a session under trace.principal; a NACKed push always gets an
 * error handoff span. No raw principal or payload in attributes.
 */
public final class GatewayTracing {

  static final AttributeKey<String> MSG_ID = AttributeKey.stringKey("pangate.msg_id");
  static final AttributeKey<String> CLASS = AttributeKey.stringKey("pangate.delivery_class");
  static final AttributeKey<String> TENANT = AttributeKey.stringKey("pangate.tenant");
  static final AttributeKey<String> PRINCIPAL = AttributeKey.stringKey("pangate.principal_hash");
  static final AttributeKey<String> NACK = AttributeKey.stringKey("pangate.nack");

  private static final Pattern TRACEPARENT = Pattern.compile("^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$");
  private static final TextMapGetter<String> GETTER = new TextMapGetter<>() {
    @Override
    public Iterable<String> keys(String carrier) {
      return List.of("traceparent");
    }

    @Override
    public String get(String carrier, String key) {
      return "traceparent".equals(key) ? carrier : null;
    }
  };

  private final Tracer tracer;  // null = no-op

  private GatewayTracing(Tracer tracer) {
    this.tracer = tracer;
  }

  private static volatile GatewayTracing current = new GatewayTracing(null);

  /** The node's tracing, for NACKs raised where no PushDelivery is at hand (pending window timeout, close). */
  public static void install(GatewayTracing t) {
    current = t;
  }

  public static GatewayTracing current() {
    return current;
  }

  public static GatewayTracing noop() {
    return new GatewayTracing(null);
  }

  public static GatewayTracing create(OpenTelemetry otel) {
    return new GatewayTracing(otel.getTracer("pangate-gateway"));
  }

  public boolean enabled() {
    return tracer != null;
  }

  /** W3C traceparent with the sampled flag; an invalid header (or all-zero ids, version ff) is not sampled. */
  public static boolean sampled(String traceparent) {
    if (traceparent == null) {
      return false;
    }
    var m = TRACEPARENT.matcher(traceparent);
    if (!m.matches() || m.group(1).equals("ff") || m.group(2).chars().allMatch(c -> c == '0')
        || m.group(3).chars().allMatch(c -> c == '0')) {
      return false;
    }
    return (Integer.parseInt(m.group(4), 16) & 1) == 1;
  }

  /** Whether this push of this session gets spans. */
  public boolean wants(Frame f, Session s) {
    return tracer != null && (s.traced() || sampled(f.getHeadersOrDefault("traceparent", null)));
  }

  private SpanBuilder builder(String name, Frame f, String tenant, String principal, long enqueuedAtMs) {
    String tp = f.getHeadersOrDefault("traceparent", null);
    Context remote = tp != null && TRACEPARENT.matcher(tp).matches()
        ? W3CTraceContextPropagator.getInstance().extract(Context.root(), tp, GETTER)
        : Context.root();
    // A sampled caller: be its child. An unsampled one: the SDK's parent-based sampler would drop a child, but these
    // spans are wanted anyway (trace.principal, NACK) — start a new trace and link to the caller's span instead.
    SpanBuilder b = tracer.spanBuilder(name);
    if (sampled(tp)) {
      b.setParent(remote);
    } else {
      b.setNoParent();
      io.opentelemetry.api.trace.SpanContext caller = Span.fromContext(remote).getSpanContext();
      if (caller.isValid()) {
        b.addLink(caller);
      }
    }
    b
        .setAttribute(MSG_ID, f.getMsgId())
        .setAttribute(CLASS, f.getClass_().name())
        .setAttribute(TENANT, tenant)
        .setAttribute(PRINCIPAL, SessionTrace.hash(principal));
    if (enqueuedAtMs > 0) {
      b.setStartTimestamp(enqueuedAtMs, TimeUnit.MILLISECONDS);
    }
    return b;
  }

  /** Starts at the router's enqueue time; the caller ends it when it writes. */
  public Span handoff(Frame f, Session s, long enqueuedAtMs) {
    return builder("gw.push.handoff", f, s.identity().tenantId(), s.identity().principalId(), enqueuedAtMs)
        .startSpan();
  }

  public Span write(Span handoff) {
    return tracer.spanBuilder("gw.push.write").setParent(Context.root().with(handoff)).startSpan();
  }

  /** A pushed entry NACKed from the pending window (timeout, close): an error span with what the session knows. */
  public void nackPending(Session s, String msgId, AckItem.Status status) {
    if (tracer == null || s.identity() == null) {
      return;
    }
    Span span = tracer.spanBuilder("gw.push.handoff").setNoParent()
        .setAttribute(MSG_ID, msgId)
        .setAttribute(TENANT, s.identity().tenantId())
        .setAttribute(PRINCIPAL, SessionTrace.hash(s.identity().principalId()))
        .setAttribute(NACK, status.name()).startSpan();
    span.setStatus(StatusCode.ERROR, status.name());
    span.end();
  }

  /** A NACKed push: one finished error span, whether or not the caller sampled the trace. */
  public void nack(PushItem item, AckItem.Status status) {
    if (tracer == null) {
      return;
    }
    Span s = builder("gw.push.handoff", item.getFrame(), item.getTenantId(), item.getPrincipalId(),
        item.getEnqueuedAtMs()).setAttribute(NACK, status.name()).startSpan();
    s.setStatus(StatusCode.ERROR, status.name());
    s.end();
  }
}
