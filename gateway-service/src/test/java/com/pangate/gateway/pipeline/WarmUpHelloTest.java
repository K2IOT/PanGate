package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.ErrorInfo;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Hello;
import com.pangate.gateway.auth.JwtResult;
import com.pangate.gateway.auth.WarmUpTokens;
import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.FrameCodec;
import com.pangate.gateway.registry.SeqReader;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionListener;
import com.pangate.gateway.test.AuthFixture;
import com.pangate.gateway.test.LoopFixture;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class WarmUpHelloTest {

  private final LoopFixture f = new LoopFixture();
  private final AuthFixture auth = new AuthFixture(f::wallNow);
  private final com.pangate.gateway.auth.PrincipalHandshakeLimiter limiter =
      new com.pangate.gateway.auth.PrincipalHandshakeLimiter(20, () -> f.now() * 1000);
  private final List<String> events = new ArrayList<>();
  private final List<String> seqReads = new ArrayList<>();
  private Session session;

  private void connect(String remote) throws Exception {
    f.listeners.add(new SessionListener() {
      @Override public void activated(Session s) { events.add("activated"); }
      @Override public void closed(Session s) { events.add("closed"); }
    });
    SeqReader seq = new SeqReader() {
      @Override public CompletionStage<OptionalLong> read(String principal) {
        seqReads.add(principal);
        return CompletableFuture.completedFuture(OptionalLong.of(1));
      }

      @Override public int seqEpoch(String principal) {
        return 0;
      }
    };
    f.channel.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.TCP);
    f.channel.attr(ChannelAttrs.PROTOCOL_READY).set(true);
    session = f.tracker.admit(f.channel, f.loop, InetAddress.getByName(remote));
    f.channel.pipeline().addLast(new FrameCodec(f.limits),
        new HelloAuthHandler(auth.tickets, auth.flow, f.tracker, f.policy(limiter), f.tenants, f.limits, "cell-1",
            f.metrics, seq), new ErrorTerminator());
  }

  private Frame send(String jwt, String device) throws Exception {
    Hello h = Hello.newBuilder().setJwt(jwt).setDeviceId(device).setSdkVersion("1.0").build();
    f.channel.writeInbound(Unpooled.wrappedBuffer(
        Frame.newBuilder().setType(FrameType.FRAME_TYPE_HELLO).setPayload(h.toByteString()).build().toByteArray()));
    f.channel.runPendingTasks();
    Frame last = null;
    for (Object o; (o = f.channel.readOutbound()) != null; ) {
      ByteBuf b = (ByteBuf) o;
      last = Frame.parseFrom(ByteBufUtil.getBytes(b));
      b.release();
    }
    return last;
  }

  @Test
  void warmUpTokenVerifiesAndClaimsReservedTenant() {
    JwtResult r = auth.verifier.verify(auth.warmUp.mint(3, f.wallNow()), f.wallNow());
    assertThat(r).isInstanceOfSatisfying(JwtResult.Verified.class, v -> {
      assertThat(v.claims().tenant()).isEqualTo(WarmUpTokens.TENANT);
      assertThat(v.claims().principal()).isEqualTo("__warmup:3");
      assertThat(v.claims().device()).isEqualTo("warmup-3");
    });
  }

  @Test
  void warmUpSessionSkipsListenersSeqAndBusinessMetrics() throws Exception {
    connect("127.0.0.1");
    Frame out = send(auth.warmUp.mint(1, f.wallNow()), "warmup-1");
    assertThat(out.getType()).isEqualTo(FrameType.FRAME_TYPE_CONNECTED);
    assertThat(session.warmUp()).isTrue();
    f.channel.close();
    f.channel.runPendingTasks();
    assertThat(events).isEmpty();
    assertThat(seqReads).isEmpty();
    assertThat(f.meters.counter("gw.handshakes", "result", "warmup").count()).isEqualTo(1);
    assertThat(f.meters.counter("gw.handshakes", "result", "ok").count()).isZero();
  }

  @Test
  void warmUpTokenFromRemoteIsRejected() throws Exception {
    connect("10.0.0.1");
    Frame out = send(auth.warmUp.mint(1, f.wallNow()), "warmup-1");
    assertThat(ErrorInfo.parseFrom(out.getPayload()).getCode()).isEqualTo(4001);
    assertThat(f.channel.isOpen()).isFalse();
  }

  @Test
  void realJwtClaimingWarmUpTenantIsRejected() throws Exception {
    connect("127.0.0.1");
    Frame out = send(auth.jwt("__warmup:x", "d1", f.wallNow()), "d1");
    assertThat(ErrorInfo.parseFrom(out.getPayload()).getCode()).isEqualTo(4001);
  }
}
