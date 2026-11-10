package com.pangate.pushapi.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.testkit.Fixtures;
import com.pangate.contracts.testkit.PushRecordRules;
import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushResponse.Code;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class PushAcceptorTest {

  static final long NOW = 1_800_000_000_000L;
  static final TenantPolicy DEFAULTS = new TenantPolicy(false, true, 1000, 2000, 10);
  static final Caller API = new Caller.ApiCaller("t1", "t1-backend", Set.of("push:send", "push:broadcast"));
  static final Caller GATEWAY = new Caller.InternalCaller("spiffe://pangate/cell-1/gateway");

  // --- fakes ---------------------------------------------------------------------------------------------------
  final Map<String, Long> counters = new HashMap<>();
  boolean storeDown;
  final SeqStore seqStore = key -> {
    if (storeDown) {
      throw new StoreUnavailableException("down", null);
    }
    return counters.merge(key, 1L, Long::sum);
  };
  final SeqEpochs epochs = new SeqEpochs() {
    @Override public int epoch(String p) { return 2; }
    @Override public void refreshNow() { }
  };
  final Map<String, String> idemValues = new HashMap<>();
  final IdemStore idemStore = new IdemStore() {
    @Override public Optional<String> get(String k) { return Optional.ofNullable(idemValues.get(k)); }
    @Override public boolean setIfAbsent(String k, String v, Duration t) { return idemValues.putIfAbsent(k, v) == null; }
    @Override public void set(String k, String v, Duration t) { idemValues.put(k, v); }
    @Override public boolean deleteIfEquals(String k, String v) { return idemValues.remove(k, v); }
  };
  final List<PushRecord> produced = new ArrayList<>();
  int failProduces;
  final RecordProducer producer = r -> {
    if (failProduces > 0) {
      failProduces--;
      throw new ProduceFailedException("broker down", null);
    }
    produced.add(r);
  };
  final List<String> statuses = new ArrayList<>();
  boolean statusDown;
  final MessageStatus status = new MessageStatus() {
    @Override public void accepted(PushRecord r) {
      if (statusDown) {
        throw new StoreUnavailableException("down", null);
      }
      statuses.add(r.getMsgId());
    }
    @Override public Optional<Map<String, String>> get(String t, String id) { return Optional.empty(); }
    @Override public void offline(OfflineRecord o) { }
    @Override public void delivered(DeliveredRecord d) { }
  };
  final AtomicLong mono = new AtomicLong();
  final ControlState control = new ControlState("c1", DEFAULTS, () -> NOW);
  RecordBuilder builder = new RecordBuilder("c1", () -> NOW, Duration.ofDays(1));

  PushAcceptor acceptor() {
    return new PushAcceptor(new SeqAllocator(seqStore, epochs, () -> { }), builder,
        new Idempotency(idemStore, Duration.ofHours(1), Duration.ofSeconds(30), Duration.ofMillis(100),
            d -> mono.addAndGet(d.toMillis()), mono::get),
        control, new RateLimiter(() -> mono.get() * 1_000_000L), producer, status, () -> NOW);
  }

  static PushRequest message() {
    return Fixtures.request(PushRequest.Kind.KIND_MESSAGE);
  }

  // --- tests ---------------------------------------------------------------------------------------------------

  @Test
  void reliableMessageGetsSeqProducesAndRecordsAccepted() {
    PushResponse r = acceptor().accept(API, message());
    assertThat(r.getCode()).isEqualTo(Code.CODE_OK);
    assertThat(r.getOk()).isTrue();
    assertThat(r.getRequestId()).isEqualTo("r-1");
    assertThat(r.getSeq()).isEqualTo(1);
    assertThat(r.getSeqEpoch()).isEqualTo(2);
    assertThat(r.getCellId()).isEqualTo("c1");
    assertThat(r.getMsgId()).hasSize(26);
    assertThat(produced).singleElement().satisfies(p -> {
      assertThat(p.getMsgId()).isEqualTo(r.getMsgId());
      assertThat(p.getFrame().getSeq()).isEqualTo(1);
      PushRecordRules.assertValid(p);
    });
    assertThat(statuses).containsExactly(r.getMsgId());
  }

  @Test
  void invalidRequestIsInvalidWithTheRule() {
    PushResponse r = acceptor().accept(API, message().toBuilder().clearPayload().build());
    assertThat(r.getCode()).isEqualTo(Code.CODE_INVALID);
    assertThat(r.getOk()).isFalse();
    assertThat(r.getError()).startsWith("V2");
    assertThat(produced).isEmpty();
  }

  @Test
  void apiCallersTenantWins() {
    PushResponse r = acceptor().accept(API, message().toBuilder().setTenantId("t2").setPrincipalId("t2:u1").build());
    assertThat(r.getCode()).isEqualTo(Code.CODE_INVALID);   // principal t2:u1 is outside the token's tenant t1 (V5)
    assertThat(produced).isEmpty();
  }

  @Test
  void controlOverHttpIsForbiddenButOverGrpcOk() {
    assertThat(acceptor().accept(API, Fixtures.kickRequest010()).getCode()).isEqualTo(Code.CODE_FORBIDDEN);
    PushResponse r = acceptor().accept(GATEWAY, Fixtures.kickRequest010());
    assertThat(r.getCode()).isEqualTo(Code.CODE_OK);
    assertThat(produced).singleElement().extracting(PushRecord::getKind).isEqualTo(PushRecord.Kind.KIND_CONTROL);
    assertThat(statuses).isEmpty();
  }

  @Test
  void scopesGateKinds() {
    Caller sendOnly = new Caller.ApiCaller("t1", "c", Set.of("push:send"));
    Caller broadcastOnly = new Caller.ApiCaller("t1", "c", Set.of("push:broadcast"));
    assertThat(acceptor().accept(broadcastOnly, message()).getCode()).isEqualTo(Code.CODE_FORBIDDEN);
    assertThat(acceptor().accept(sendOnly, Fixtures.request(PushRequest.Kind.KIND_BROADCAST)).getCode())
        .isEqualTo(Code.CODE_FORBIDDEN);
    assertThat(acceptor().accept(broadcastOnly, Fixtures.request(PushRequest.Kind.KIND_BROADCAST)).getCode())
        .isEqualTo(Code.CODE_OK);
  }

  @Test
  void blockedTenantIsForbiddenExceptInternalControl() {
    control.apply("tenantcfg:tenant:t1", ControlStateTest.cfg(com.pangate.contracts.v1.Scope.SCOPE_TENANT, "t1", null,
        com.pangate.contracts.v1.KillSwitches.newBuilder().setTenantBlocked(true).build()));
    PushResponse r = acceptor().accept(API, message());
    assertThat(r.getCode()).isEqualTo(Code.CODE_FORBIDDEN);
    assertThat(r.getError()).contains("tenant_blocked");
    assertThat(acceptor().accept(GATEWAY, message()).getCode()).isEqualTo(Code.CODE_FORBIDDEN);
    assertThat(acceptor().accept(GATEWAY, Fixtures.kickRequest010()).getCode()).isEqualTo(Code.CODE_OK);
  }

  @Test
  void broadcastDisabledIsForbidden() {
    control.apply("tenantcfg:fleet", ControlStateTest.cfg(com.pangate.contracts.v1.Scope.SCOPE_FLEET, "", null,
        com.pangate.contracts.v1.KillSwitches.newBuilder().setBroadcastEnabled(false).build()));
    assertThat(acceptor().accept(API, Fixtures.request(PushRequest.Kind.KIND_BROADCAST)).getCode())
        .isEqualTo(Code.CODE_FORBIDDEN);
    assertThat(acceptor().accept(API, message()).getCode()).isEqualTo(Code.CODE_OK);
  }

  @Test
  void rateLimitedCarriesRetryAfter() {
    control.apply("tenantcfg:tenant:t1", ControlStateTest.cfg(com.pangate.contracts.v1.Scope.SCOPE_TENANT, "t1",
        com.pangate.contracts.v1.TenantLimits.newBuilder().setPushRate(2).setPushBurst(1).build(), null));
    PushAcceptor a = acceptor();
    assertThat(a.accept(API, message()).getCode()).isEqualTo(Code.CODE_OK);
    PushResponse r = a.accept(API, message());
    assertThat(r.getCode()).isEqualTo(Code.CODE_RATE_LIMITED);
    assertThat(r.getRetryAfterMs()).isEqualTo(500);
    assertThat(produced).hasSize(1);
  }

  @Test
  void produceFailureIsUnavailableAndAbandonsTheKey() {
    failProduces = 1;
    PushResponse r = acceptor().accept(API, message().toBuilder().setIdempotencyKey("k1").build());
    assertThat(r.getCode()).isEqualTo(Code.CODE_UNAVAILABLE);
    assertThat(r.getRetryAfterMs()).isEqualTo(500);
    assertThat(idemValues).doesNotContainKey("idem:{t1}:k1").containsKey("idemlast:{t1}:k1");
  }

  /** Review Focus 1. */
  @Test
  void retryAfterFailureProducesOnceWithTheReservedValues() {
    failProduces = 1;
    PushAcceptor a = acceptor();
    PushRequest req = message().toBuilder().setIdempotencyKey("k1").build();
    a.accept(API, req);
    PushResponse ok = a.accept(API, req);
    assertThat(ok.getCode()).isEqualTo(Code.CODE_OK);
    assertThat(produced).hasSize(1);
    assertThat(ok.getSeq()).isEqualTo(1);
    assertThat(counters).containsEntry("seq:{t1:u1}", 1L);   // the retry took no new seq
    PushResponse dup = a.accept(API, req);
    assertThat(dup.getDuplicate()).isTrue();
    assertThat(dup.getMsgId()).isEqualTo(ok.getMsgId());
    assertThat(produced).hasSize(1);
  }

  /** Final review: a retry with the same key but another target must not carry the first target's seq. */
  @Test
  void retryWithTheSameKeyForAnotherPrincipalTakesFreshValues() {
    failProduces = 1;
    counters.put("seq:{t1:u2}", 5L);
    PushAcceptor a = acceptor();
    a.accept(API, message().toBuilder().setIdempotencyKey("k1").build());
    PushResponse r = a.accept(API, message().toBuilder().setIdempotencyKey("k1").setPrincipalId("t1:u2").build());
    assertThat(r.getCode()).isEqualTo(Code.CODE_OK);
    assertThat(r.getSeq()).isEqualTo(6);
    assertThat(produced).singleElement().satisfies(p -> assertThat(p.getFrame().getSeq()).isEqualTo(6));
  }

  /** Final review: a retry that changes the kind is a new request, not a 500 for the next hour. */
  @Test
  void retryWithTheSameKeyAsAnotherKindIsAccepted() {
    failProduces = 1;
    PushAcceptor a = acceptor();
    a.accept(API, message().toBuilder().setIdempotencyKey("k1").build());
    PushResponse r = a.accept(API, Fixtures.request(PushRequest.Kind.KIND_BROADCAST).toBuilder()
        .setIdempotencyKey("k1").build());
    assertThat(r.getCode()).isEqualTo(Code.CODE_OK);
    assertThat(r.getTopicSeq()).isEqualTo(1);
  }

  /** Final review minor: an unbounded key becomes two unbounded Redis keys for an hour. */
  @Test
  void idempotencyKeyOver256BytesIsInvalid() {
    PushResponse r = acceptor().accept(API, message().toBuilder().setIdempotencyKey("k".repeat(257)).build());
    assertThat(r.getCode()).isEqualTo(Code.CODE_INVALID);
    assertThat(produced).isEmpty();
    assertThat(acceptor().accept(API, message().toBuilder().setIdempotencyKey("é".repeat(128)).build()).getCode())
        .isEqualTo(Code.CODE_OK);
  }

  @Test
  void duplicateIsReturnedWithoutProducing() {
    PushAcceptor a = acceptor();
    PushRequest req = message().toBuilder().setIdempotencyKey("k2").build();
    PushResponse first = a.accept(API, req);
    PushResponse again = a.accept(API, req.toBuilder().setRequestId("r-2").build());
    assertThat(again.getDuplicate()).isTrue();
    assertThat(again.getMsgId()).isEqualTo(first.getMsgId());
    assertThat(again.getRequestId()).isEqualTo("r-2");
    assertThat(produced).hasSize(1);
  }

  @Test
  void busyKeyIsUnavailable() {
    idemValues.put("idem:{t1}:k3", "pending|M|1|0|0");
    PushResponse r = acceptor().accept(API, message().toBuilder().setIdempotencyKey("k3").build());
    assertThat(r.getCode()).isEqualTo(Code.CODE_UNAVAILABLE);
    assertThat(r.getRetryAfterMs()).isEqualTo(200);
  }

  @Test
  void statusFailureDoesNotFailTheRequest() {
    statusDown = true;
    assertThat(acceptor().accept(API, message()).getCode()).isEqualTo(Code.CODE_OK);
    assertThat(produced).hasSize(1);
  }

  @Test
  void storeTimeoutIsUnavailable() {
    storeDown = true;
    PushResponse r = acceptor().accept(API, message());
    assertThat(r.getCode()).isEqualTo(Code.CODE_UNAVAILABLE);
    assertThat(r.getRetryAfterMs()).isEqualTo(200);
    assertThat(acceptor().accept(API, message().toBuilder().setClass_(DeliveryClass.DELIVERY_CLASS_EPHEMERAL).build())
        .getCode()).isEqualTo(Code.CODE_OK);     // EPHEMERAL without a key needs no Redis
  }

  @Test
  void broadcastGetsTopicSeq() {
    PushResponse r = acceptor().accept(API, Fixtures.request(PushRequest.Kind.KIND_BROADCAST));
    assertThat(r.getTopicSeq()).isEqualTo(1);
    assertThat(counters).containsKey("topicseq:{t1:news}");
  }

  @Test
  void builtRecordBreakingRulesIsNeverProduced() {
    builder = new RecordBuilder("c1", () -> NOW, Duration.ofDays(1)) {
      @Override
      public PushRecord build(PushRequest r, String msgId, Seq seq, long topicSeq) {
        return super.build(r, msgId, seq, topicSeq).toBuilder().setMsgId("").build();
      }
    };
    assertThatThrownBy(() -> acceptor().accept(API, message())).isInstanceOf(IllegalStateException.class);
    assertThat(produced).isEmpty();
  }

  @Test
  void ruleCheckAgreesWithTheTestkit() {
    PushRecord good = new RecordBuilder("c1", () -> NOW, Duration.ofDays(1)).build(message(), "M", new Seq(1, 0), 0);
    assertThat(PushRecordCheck.violation(good)).isEmpty();
    for (PushRecord bad : List.of(good.toBuilder().setMsgId("").build(), good.toBuilder().setTenantId("t2").build(),
        good.toBuilder().clearFrame().build(), good.toBuilder().setExpiresAtMs(0).build(),
        good.toBuilder().setFrame(good.getFrame().toBuilder().setSeq(0)).build())) {
      boolean testkit;
      try {
        PushRecordRules.assertValid(bad);
        testkit = false;
      } catch (AssertionError e) {
        testkit = true;
      }
      assertThat(PushRecordCheck.violation(bad).isPresent()).as(bad.toString()).isEqualTo(testkit).isTrue();
    }
  }
}
