package com.pangate.pushapi.core;

import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushResponse.Code;
import java.util.Optional;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One push request from either surface to a produced {@link PushRecord} (push-api spec §2, ADR 0022): rules,
 * authorization, kill switch, rate limit, idempotency, seq, record, produce, status. Every expected outcome is a
 * {@link PushResponse}; only a record that breaks the record rules (a bug) throws.
 */
public final class PushAcceptor {

  private static final Logger log = LoggerFactory.getLogger(PushAcceptor.class);
  static final int RETRY_PRODUCE_MS = 500;
  static final int RETRY_STORE_MS = 200;
  /** Bounds the two Redis keys a client key makes for an hour (same bound as {@code collapse_key}). */
  static final int MAX_IDEMPOTENCY_KEY_BYTES = 256;

  private final SeqAllocator seq;
  private final RecordBuilder builder;
  private final Idempotency idempotency;
  private final ControlState control;
  private final RateLimiter limiter;
  private final RecordProducer producer;
  private final MessageStatus status;
  private final LongSupplier wallMs;

  public PushAcceptor(SeqAllocator seq, RecordBuilder builder, Idempotency idempotency, ControlState control,
      RateLimiter limiter, RecordProducer producer, MessageStatus status, LongSupplier wallMs) {
    this.seq = seq;
    this.builder = builder;
    this.idempotency = idempotency;
    this.control = control;
    this.limiter = limiter;
    this.producer = producer;
    this.status = status;
    this.wallMs = wallMs;
  }

  public PushResponse accept(Caller caller, PushRequest request) {
    PushRequest r = caller instanceof Caller.ApiCaller api && !api.tenant().equals(request.getTenantId())
        ? request.toBuilder().setTenantId(api.tenant()).build() : request;
    PushResponse.Builder res = PushResponse.newBuilder().setRequestId(r.getRequestId());
    Optional<Violation> v = PushRequests.violation(r);
    if (v.isPresent()) {
      return fail(res, Code.CODE_INVALID, v.get().rule() + ": " + v.get().message());
    }
    if (r.getIdempotencyKey().getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_IDEMPOTENCY_KEY_BYTES) {
      return fail(res, Code.CODE_INVALID, "idempotency_key: at most 256 bytes");
    }
    boolean internalControl = caller instanceof Caller.InternalCaller && r.getKind() == PushRequest.Kind.KIND_CONTROL;
    Optional<String> forbidden = forbidden(caller, r);
    if (forbidden.isPresent()) {
      return fail(res, Code.CODE_FORBIDDEN, forbidden.get());
    }
    TenantPolicy policy = control.policy(r.getTenantId());
    if (!internalControl && policy.blocked()) {
      return fail(res, Code.CODE_FORBIDDEN, "tenant_blocked: push is disabled for this tenant");
    }
    if (r.getKind() == PushRequest.Kind.KIND_BROADCAST && !policy.broadcastEnabled()) {
      return fail(res, Code.CODE_FORBIDDEN, "broadcast_disabled: broadcast is disabled for this tenant");
    }
    long waitNanos = internalControl ? 0 : limiter.tryAcquire(r.getTenantId(), r.getKind(), policy);
    if (waitNanos > 0) {
      return fail(res, Code.CODE_RATE_LIMITED, "rate_limited")
          .toBuilder().setRetryAfterMs((int) Math.max(1, Math.min(Integer.MAX_VALUE, (waitNanos + 999_999) / 1_000_000)))
          .build();
    }
    try {
      return produce(r, res);
    } catch (StoreUnavailableException e) {
      log.warn("push {} for tenant {}: state store unavailable: {}", r.getKind(), r.getTenantId(), e.toString());
      return fail(res, Code.CODE_UNAVAILABLE, "unavailable: state store").toBuilder()
          .setRetryAfterMs(RETRY_STORE_MS).build();
    }
  }

  private static Optional<String> forbidden(Caller caller, PushRequest r) {
    if (!(caller instanceof Caller.ApiCaller api)) {
      return Optional.empty();
    }
    return switch (r.getKind()) {
      case KIND_CONTROL -> Optional.of("forbidden: CONTROL is only accepted over gRPC");
      case KIND_BROADCAST -> api.scopes().contains("push:broadcast") ? Optional.empty()
          : Optional.of("forbidden: scope push:broadcast required");
      default -> api.scopes().contains("push:send") ? Optional.empty()
          : Optional.of("forbidden: scope push:send required");
    };
  }

  private PushResponse produce(PushRequest r, PushResponse.Builder res) {
    Idempotency.Owned owned = null;
    Reserved reserved;
    if (!r.getIdempotencyKey().isEmpty()) {
      Idempotency.Claim c = idempotency.claim(r.getTenantId(), r.getIdempotencyKey(), reservedFor(r), () -> reserve(r));
      switch (c) {
        case Idempotency.Done d -> {
          return d.first().toBuilder().setRequestId(r.getRequestId()).build();
        }
        case Idempotency.Busy b -> {
          return fail(res, Code.CODE_UNAVAILABLE, "unavailable: the same Idempotency-Key is in progress").toBuilder()
              .setRetryAfterMs(RETRY_STORE_MS).build();
        }
        case Idempotency.Owned o -> owned = o;
      }
      reserved = owned.reserved();
    } else {
      reserved = reserve(r);
    }
    PushRecord rec = builder.build(r, reserved.msgId(),
        reserved.seq() > 0 ? new Seq(reserved.seq(), reserved.seqEpoch()) : null, reserved.topicSeq());
    Optional<String> broken = PushRecordCheck.violation(rec);
    if (broken.isPresent()) {
      throw new IllegalStateException("built a PushRecord that breaks §1.1 (" + broken.get() + ")");
    }
    try {
      producer.send(rec);
    } catch (ProduceFailedException e) {
      if (owned != null) {
        try {
          idempotency.abandon(owned);
        } catch (StoreUnavailableException ignored) {
          // the pending value expires on its own
        }
      }
      log.warn("push {} msg_id {} for tenant {} not produced: {}", r.getKind(), rec.getMsgId(), r.getTenantId(),
          e.toString());
      return fail(res, Code.CODE_UNAVAILABLE, "unavailable: kafka").toBuilder().setRetryAfterMs(RETRY_PRODUCE_MS)
          .build();
    }
    if (rec.getKind() != PushRecord.Kind.KIND_CONTROL) {
      try {
        status.accepted(rec);
      } catch (RuntimeException e) {
        log.warn("status of msg_id {} not recorded: {}", rec.getMsgId(), e.toString());
      }
    }
    PushResponse ok = res.setOk(true).setCode(Code.CODE_OK).setMsgId(rec.getMsgId()).setSeq(reserved.seq())
        .setSeqEpoch(reserved.seq() > 0 ? reserved.seqEpoch() : 0).setTopicSeq(reserved.topicSeq())
        .setCellId(rec.getCellId()).build();
    if (owned != null) {
      try {
        idempotency.complete(owned, ok);
      } catch (StoreUnavailableException e) {
        log.warn("idempotency key of msg_id {} not completed: {}", rec.getMsgId(), e.toString());
      }
    }
    return ok;
  }

  /** What reserved values are for: a retry with the same key reuses them only for the same kind, class and target. */
  static String reservedFor(PushRequest r) {
    return r.getKindValue() + "/" + r.getClass_Value() + "/" + part(r.getPrincipalId()) + part(r.getDeviceId())
        + part(r.getTopic());
  }

  private static String part(String s) {
    return s.length() + ":" + s;
  }

  private Reserved reserve(PushRequest r) {
    String msgId = Ulids.next(wallMs.getAsLong());
    boolean sequenced = r.getClass_() != DeliveryClass.DELIVERY_CLASS_EPHEMERAL;
    if (r.getKind() == PushRequest.Kind.KIND_MESSAGE && r.getDeviceId().isEmpty() && sequenced) {
      Seq s = seq.forPrincipal(r.getPrincipalId());
      return new Reserved(msgId, s.seq(), s.epoch(), 0);
    }
    if (r.getKind() == PushRequest.Kind.KIND_BROADCAST && sequenced) {
      return new Reserved(msgId, 0, 0, seq.forTopic(r.getTopic()));
    }
    return new Reserved(msgId, 0, 0, 0);
  }

  private static PushResponse fail(PushResponse.Builder res, Code code, String error) {
    return res.setOk(false).setCode(code).setError(error).build();
  }
}
