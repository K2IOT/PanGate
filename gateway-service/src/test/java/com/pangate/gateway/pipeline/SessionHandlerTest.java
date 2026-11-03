package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.jwk.ECKey;
import com.pangate.contracts.v1.ErrorInfo;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Reauth;
import com.pangate.gateway.auth.JwtResult;
import com.pangate.gateway.auth.TicketVerifyResult;
import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.AuthFixture;
import com.pangate.gateway.test.LoopFixture;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SessionHandlerTest {

  private final LoopFixture f = new LoopFixture();
  private final AuthFixture auth = new AuthFixture(f::now);
  private final com.pangate.gateway.auth.PrincipalHandshakeLimiter limiter =
      new com.pangate.gateway.auth.PrincipalHandshakeLimiter(20, () -> f.now() * 1000);
  private Session session;

  @BeforeEach
  void activeSession() {
    f.channel.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.TCP);
    f.channel.attr(ChannelAttrs.PROTOCOL_READY).set(true);
    f.channel.attr(ChannelAttrs.AUTHENTICATED).set(true);
    session = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    session.activate(new Session.Identity("t1", "t1:u1", "d1", "ios", "1.0", Set.of()), "CONN",
        f.now() - 100, f.now() + 3_600, 7, List.of("t1:news.*"));
    f.channel.pipeline().addLast(new SessionHandler(auth.tickets, auth.flow, limiter, f.limits, "cell-1"), new ErrorTerminator());
  }

  private Frame in(Frame frame) {
    f.channel.writeInbound(frame);
    f.channel.runPendingTasks();
    return f.channel.readOutbound();
  }

  private static Frame frame(FrameType t) {
    return Frame.newBuilder().setType(t).build();
  }

  @Test
  void pingEchoesMsgIdAndTouches() {
    f.clockSec.addAndGet(40);

    Frame out = in(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PING).setMsgId("p1").build());

    assertThat(out.getType()).isEqualTo(FrameType.FRAME_TYPE_PONG);
    assertThat(out.getMsgId()).isEqualTo("p1");
    assertThat(session.lastSeenSec()).isEqualTo(f.now());
  }

  @Test
  void secondHelloIsBadFrame() throws Exception {
    Frame out = in(frame(FrameType.FRAME_TYPE_HELLO));

    assertThat(ErrorInfo.parseFrom(out.getPayload()).getCode()).isEqualTo(4000);
    assertThat(f.channel.isOpen()).isFalse();
  }

  @Test
  void reauthExtendsExpiryAndReturnsNewTicket() throws Exception {
    Frame req = Frame.newBuilder().setType(FrameType.FRAME_TYPE_REAUTH)
        .setPayload(Reauth.newBuilder().setJwt(auth.jwt("t1:u1", "d1", f.now())).build().toByteString()).build();

    Frame out = in(req);

    assertThat(out.getType()).isEqualTo(FrameType.FRAME_TYPE_REAUTH);
    Reauth reply = Reauth.parseFrom(out.getPayload());
    assertThat(reply.getSessionExpiresAtMs()).isEqualTo((f.now() + 86_400) * 1000);
    assertThat(session.expAtSec()).isEqualTo(f.now() + 86_400);
    TicketVerifyResult t = auth.tickets.verify(reply.getResumeTicket(), f.now());
    assertThat(t).isInstanceOfSatisfying(TicketVerifyResult.Valid.class, v -> {
      assertThat(v.ticket().authAtSec()).isEqualTo(f.now());
      assertThat(v.ticket().principal()).isEqualTo("t1:u1");
      assertThat(v.ticket().cell()).isEqualTo("cell-1");
    });
  }

  @Test
  void reauthForOtherPrincipalAuthFailed() throws Exception {
    Frame req = Frame.newBuilder().setType(FrameType.FRAME_TYPE_REAUTH)
        .setPayload(Reauth.newBuilder().setJwt(auth.jwt("t1:other", "d1", f.now())).build().toByteString()).build();

    Frame out = in(req);

    assertThat(ErrorInfo.parseFrom(out.getPayload()).getCode()).isEqualTo(4001);
    assertThat(session.closeCode()).isEqualTo(CloseCode.AUTH_FAILED);
  }

  @Test
  void reauthBeyondTwentyPerMinuteIsRateLimited() throws Exception {
    for (int i = 0; i < 20; i++) {
      limiter.tryAcquire("t1:u1");
    }
    Frame req = Frame.newBuilder().setType(FrameType.FRAME_TYPE_REAUTH)
        .setPayload(Reauth.newBuilder().setJwt(auth.jwt("t1:u1", "d1", f.now())).build().toByteString()).build();

    Frame out = in(req);

    ErrorInfo e = ErrorInfo.parseFrom(out.getPayload());
    assertThat(e.getCode()).isEqualTo(4009);
    assertThat(e.getRetryAfterMs()).isEqualTo(30_000);
    assertThat(session.closeCode()).isEqualTo(CloseCode.RATE_LIMITED);
  }

  @Test
  void unsupportedFramesGetNonFatalError() throws Exception {
    for (FrameType t : List.of(FrameType.FRAME_TYPE_UPSTREAM, FrameType.FRAME_TYPE_SUBSCRIBE,
        FrameType.FRAME_TYPE_UNSUBSCRIBE, FrameType.FRAME_TYPE_RPC_RES)) {
      Frame out = in(frame(t));
      ErrorInfo e = ErrorInfo.parseFrom(out.getPayload());
      assertThat(e.getCode()).isEqualTo(4500);
      assertThat(e.getMessage()).isEqualTo("UNSUPPORTED");
      assertThat(e.getFatal()).isFalse();
      assertThat(e.getRetryable()).isFalse();
    }
    assertThat(f.channel.isOpen()).isTrue();
  }

  @Test
  void ackIsIgnored() {
    assertThat(in(frame(FrameType.FRAME_TYPE_ACK))).isNull();
    assertThat(f.channel.isOpen()).isTrue();
  }

  @Test
  void jwtAuthFlowRefreshesUnknownKidOnLoop() throws Exception {
    ECKey rotated = AuthFixture.newKey();
    String jwt = auth.jwt(rotated, "t1:u1", "d1", f.now());
    auth.published.add(rotated);
    AtomicReference<JwtResult> result = new AtomicReference<>();
    AtomicReference<Boolean> autoReadDuringCallback = new AtomicReference<>();

    auth.flow.verify(f.channel, jwt, f.now(), r -> {
      result.set(r);
      autoReadDuringCallback.set(f.channel.config().isAutoRead());
    });
    assertThat(f.channel.config().isAutoRead()).isFalse();
    f.channel.runPendingTasks();

    assertThat(result.get()).isInstanceOf(JwtResult.Verified.class);
    assertThat(autoReadDuringCallback.get()).isFalse();
    assertThat(f.channel.config().isAutoRead()).isTrue();
  }
}
