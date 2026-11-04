package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.ErrorInfo;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Hello;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.contracts.v1.TenantLimits;
import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.FrameCodec;
import com.pangate.gateway.registry.SeqReader;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.tenant.TenantCap;
import com.pangate.gateway.tenant.TenantConnCounter;
import com.pangate.gateway.test.AuthFixture;
import com.pangate.gateway.test.LoopFixture;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.net.InetAddress;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Spec §4.6 step i with maxConns = 2 on a one-node cell: local quota ceil(2 × 1.1 / 1) = 3. */
class TenantCapHelloTest {

  private final LoopFixture f = new LoopFixture();
  private final AuthFixture auth = new AuthFixture(f::wallNow);
  private final com.pangate.gateway.auth.PrincipalHandshakeLimiter limiter =
      new com.pangate.gateway.auth.PrincipalHandshakeLimiter(20, () -> f.now() * 1000);
  private final TenantConnCounter counter = new TenantConnCounter();
  private int borrowCalls;
  private CompletableFuture<Boolean> borrowResult = CompletableFuture.completedFuture(true);
  private final TenantCap cap = new TenantCap(counter, () -> 1, (t, max, ownAfter) -> {
    borrowCalls++;
    return borrowResult;
  }, f.metrics);
  private Session session;

  private final SeqReader seq = new SeqReader() {
    @Override public CompletionStage<OptionalLong> read(String principal) {
      return CompletableFuture.completedFuture(OptionalLong.of(1));
    }

    @Override public int seqEpoch(String principal) {
      return 0;
    }
  };

  @BeforeEach
  void setUp() {
    f.listeners.add(cap);
    f.tenants.apply(TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
        .setLimits(TenantLimits.newBuilder().setMaxConns(2)).build());
    f.channel.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.TCP);
    f.channel.attr(ChannelAttrs.PROTOCOL_READY).set(true);
    session = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    f.channel.pipeline().addLast(new FrameCodec(f.limits),
        new HelloAuthHandler(auth.tickets, auth.flow, f.tracker, f.policy(limiter), f.tenants, f.limits, "cell-1",
            f.metrics, seq, cap), new ErrorTerminator());
  }

  private Frame hello() throws Exception {
    Hello h = Hello.newBuilder().setJwt(auth.jwt("t1:u1", "d1", f.wallNow())).setDeviceId("d1").setSdkVersion("1.0")
        .build();
    f.channel.writeInbound(Unpooled.wrappedBuffer(
        Frame.newBuilder().setType(FrameType.FRAME_TYPE_HELLO).setPayload(h.toByteString()).build().toByteArray()));
    return drain();
  }

  private Frame drain() throws Exception {
    f.channel.runPendingTasks();
    Frame last = null;
    for (Object o; (o = f.channel.readOutbound()) != null; ) {
      ByteBuf b = (ByteBuf) o;
      last = Frame.parseFrom(ByteBufUtil.getBytes(b));
      b.release();
    }
    return last;
  }

  private void fillLocalQuota() {
    for (int i = 0; i < 3; i++) {
      assertThat(counter.tryReserve("t1", 3)).isTrue();
    }
  }

  @Test
  void underLocalQuotaConnectsWithoutRedis() throws Exception {
    assertThat(hello().getType()).isEqualTo(FrameType.FRAME_TYPE_CONNECTED);
    assertThat(borrowCalls).isZero();
    assertThat(counter.count("t1")).isEqualTo(1);
    assertThat(session.tenantReserved()).isTrue();
  }

  @Test
  void overQuotaBorrowsAndConnects() throws Exception {
    fillLocalQuota();
    assertThat(hello().getType()).isEqualTo(FrameType.FRAME_TYPE_CONNECTED);
    assertThat(borrowCalls).isEqualTo(1);
    assertThat(counter.count("t1")).isEqualTo(4);
    assertThat(session.tenantReserved()).isTrue();
  }

  @Test
  void deniedBorrowClosesOverloadedWithRetryAfter() throws Exception {
    fillLocalQuota();
    borrowResult = CompletableFuture.completedFuture(false);
    ErrorInfo e = ErrorInfo.parseFrom(hello().getPayload());
    assertThat(e.getCode()).isEqualTo(4013);
    assertThat(e.getRetryAfterMs()).isBetween(5_000, 30_000);
    assertThat(counter.count("t1")).isEqualTo(3);
    assertThat(f.meters.counter("gw.handshakes", "result", "tenant_cap").count()).isEqualTo(1);
    assertThat(f.meters.counter("gw.tenant.borrow", "result", "denied").count()).isEqualTo(1);
  }

  @Test
  void borrowErrorIsTreatedAsDenied() throws Exception {
    fillLocalQuota();
    borrowResult = CompletableFuture.failedFuture(new IllegalStateException("redis down"));
    assertThat(ErrorInfo.parseFrom(hello().getPayload()).getCode()).isEqualTo(4013);
    assertThat(f.meters.counter("gw.tenant.borrow", "result", "error").count()).isEqualTo(1);
  }

  @Test
  void closeReleasesTheReservation() throws Exception {
    hello();
    f.channel.close();
    f.channel.runPendingTasks();
    assertThat(counter.count("t1")).isZero();
  }

  @Test
  void sessionClosedWhileBorrowingDoesNotActivate() throws Exception {
    fillLocalQuota();
    borrowResult = new CompletableFuture<>();
    hello();
    f.channel.close();
    f.channel.runPendingTasks();
    borrowResult.complete(true);
    f.channel.runPendingTasks();
    assertThat(session.identity()).isNull();
    assertThat(counter.count("t1")).isEqualTo(3);
  }
}
