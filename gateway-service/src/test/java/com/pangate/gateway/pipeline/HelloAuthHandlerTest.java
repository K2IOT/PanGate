package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.InvalidProtocolBufferException;
import com.nimbusds.jose.jwk.ECKey;
import com.pangate.contracts.v1.Connected;
import com.pangate.contracts.v1.ErrorInfo;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Hello;
import com.pangate.gateway.auth.PrincipalHandshakeLimiter;
import com.pangate.gateway.auth.ResumeTicket;
import com.pangate.gateway.auth.TicketVerifyResult;
import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.FrameCodec;
import com.pangate.gateway.session.ConnId;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import com.pangate.gateway.test.AuthFixture;
import com.pangate.gateway.test.LoopFixture;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HelloAuthHandlerTest {

  private final LoopFixture f = new LoopFixture();
  private final AuthFixture auth = new AuthFixture(f::now);
  private final PrincipalHandshakeLimiter limiter = new PrincipalHandshakeLimiter(20, () -> f.now() * 1000);
  private Session session;

  @BeforeEach
  void pipeline() {
    f.channel.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.TCP);
    f.channel.attr(ChannelAttrs.PROTOCOL_READY).set(true);
    session = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    f.channel.pipeline().addLast(new FrameCodec(f.limits),
        new HelloAuthHandler(auth.tickets, auth.flow, f.tracker, limiter, f.limits, "cell-1", f.metrics),
        new SessionHandler(auth.tickets, auth.flow, limiter, f.limits, "cell-1"), new ErrorTerminator());
  }

  private static ByteBuf bytes(Frame frame) {
    return Unpooled.wrappedBuffer(frame.toByteArray());
  }

  private static Frame hello(Hello h) {
    return Frame.newBuilder().setType(FrameType.FRAME_TYPE_HELLO).setPayload(h.toByteString()).build();
  }

  private static Hello jwtHello(String jwt, String device) {
    return Hello.newBuilder().setJwt(jwt).setDeviceId(device).setSdkVersion("1.0").setPlatform("ios").build();
  }

  private static Hello ticketHello(String ticket, String device) {
    return Hello.newBuilder().setResumeTicket(ticket).setDeviceId(device).build();
  }

  private List<Frame> outbound() throws InvalidProtocolBufferException {
    f.channel.runPendingTasks();
    List<Frame> out = new ArrayList<>();
    for (Object o; (o = f.channel.readOutbound()) != null; ) {
      ByteBuf b = (ByteBuf) o;
      out.add(Frame.parseFrom(ByteBufUtil.getBytes(b)));
      b.release();
    }
    return out;
  }

  private Connected send(Hello h) throws Exception {
    f.channel.writeInbound(bytes(hello(h)));
    List<Frame> out = outbound();
    assertThat(out).isNotEmpty();
    assertThat(out.get(0).getType()).isEqualTo(FrameType.FRAME_TYPE_CONNECTED);
    return Connected.parseFrom(out.get(0).getPayload());
  }

  private int closeCodeAfter(Hello h) throws Exception {
    f.channel.writeInbound(bytes(hello(h)));
    List<Frame> out = outbound();
    assertThat(f.channel.isOpen()).isFalse();
    return ErrorInfo.parseFrom(out.get(out.size() - 1).getPayload()).getCode();
  }

  private String ticket(String cell, long authAt, long exp) {
    return auth.tickets.issue(new ResumeTicket(cell, "t1", "t1:u1", "d1", authAt, exp, 42, List.of("t1:a.*")));
  }

  @Test
  void nonHelloBeforeAuthClosesBadFrame() throws Exception {
    f.channel.writeInbound(bytes(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PING).build()));
    List<Frame> out = outbound();

    assertThat(ErrorInfo.parseFrom(out.get(0).getPayload()).getCode()).isEqualTo(4000);
    assertThat(f.channel.isOpen()).isFalse();
  }

  @Test
  void nonFrameMessageIsReleasedAndClosesBadFrame() {
    io.netty.handler.codec.http.FullHttpRequest req = new io.netty.handler.codec.http.DefaultFullHttpRequest(
        io.netty.handler.codec.http.HttpVersion.HTTP_1_1, io.netty.handler.codec.http.HttpMethod.GET, "/x",
        Unpooled.buffer(16).writeZero(16));

    f.channel.pipeline().fireChannelRead(req);
    f.channel.runPendingTasks();

    assertThat(req.refCnt()).isZero();
    assertThat(f.channel.isOpen()).isFalse();
  }

  @Test
  void framesAfterAFailedHelloInTheSameReadAreIgnored() throws Exception {
    String good = ticket("cell-1", f.now(), f.now() + 100);
    String bad = good.substring(0, 10) + (good.charAt(10) == 'A' ? 'B' : 'A') + good.substring(11);

    f.channel.writeInbound(bytes(hello(ticketHello(bad, "d1"))), bytes(hello(ticketHello(good, "d1"))));
    List<Frame> out = outbound();

    assertThat(out).extracting(Frame::getType).containsExactly(FrameType.FRAME_TYPE_ERROR);
    assertThat(session.state()).isEqualTo(SessionState.CLOSED);
    assertThat(f.principals.principalCount()).isZero();
  }

  @Test
  void helloWithJwtSendsConnected() throws Exception {
    Connected c = send(jwtHello(auth.jwt("t1:u1", "d1", f.now()), "d1"));

    assertThat(ConnId.isValid(c.getConnId())).isTrue();
    assertThat(c.getCell()).isEqualTo("cell-1");
    assertThat(c.getHeartbeatSec()).isEqualTo(30);
    assertThat(c.getSeqUnknown()).isTrue();
    assertThat(c.getSessionExpiresAtMs()).isEqualTo((f.now() + 86_400) * 1000);
    assertThat(c.getServerTimeMs()).isPositive();
    assertThat(auth.tickets.verify(c.getResumeTicket(), f.now())).isInstanceOf(TicketVerifyResult.Valid.class);
    assertThat(session.state()).isEqualTo(SessionState.ACTIVE);
    assertThat(session.connId()).isEqualTo(c.getConnId());
    assertThat(f.principals.get("t1:u1")).hasSize(1);
    assertThat(f.channel.pipeline().get(HelloAuthHandler.class)).isNull();
    assertThat(f.channel.attr(ChannelAttrs.AUTHENTICATED).get()).isTrue();
    assertThat(f.meters.get("gw.handshakes").tag("result", "ok").counter().count()).isEqualTo(1);
  }

  @Test
  void helloWithTicketKeepsAuthAtAndExpiry() throws Exception {
    long authAt = f.now() - 3_600;
    long exp = authAt + 86_400;

    Connected c = send(ticketHello(ticket("cell-1", authAt, exp), "d1"));

    assertThat(c.getSessionExpiresAtMs()).isEqualTo(exp * 1000);
    TicketVerifyResult.Valid v = (TicketVerifyResult.Valid) auth.tickets.verify(c.getResumeTicket(), f.now());
    assertThat(v.ticket().authAtSec()).isEqualTo(authAt);
    assertThat(v.ticket().expSec()).isEqualTo(exp);
    assertThat(v.ticket().authJtiHash()).isEqualTo(42);
    assertThat(v.ticket().topicPatterns()).containsExactly("t1:a.*");
  }

  @Test
  void deviceMismatchAuthFailed() throws Exception {
    assertThat(closeCodeAfter(jwtHello(auth.jwt("t1:u1", "d1", f.now()), "d2"))).isEqualTo(4001);
  }

  @Test
  void ticketFromOtherCellWrongCell() throws Exception {
    assertThat(closeCodeAfter(ticketHello(ticket("cell-9", f.now(), f.now() + 100), "d1"))).isEqualTo(4011);
  }

  @Test
  void expiredTicketSessionExpired() throws Exception {
    assertThat(closeCodeAfter(ticketHello(ticket("cell-1", f.now() - 90_000, f.now() - 1), "d1"))).isEqualTo(4002);
  }

  @Test
  void tamperedTicketAuthFailed() throws Exception {
    String t = ticket("cell-1", f.now(), f.now() + 100);
    String tampered = t.substring(0, 10) + (t.charAt(10) == 'A' ? 'B' : 'A') + t.substring(11);

    assertThat(closeCodeAfter(ticketHello(tampered, "d1"))).isEqualTo(4001);
  }

  @Test
  void rejectedJwtAuthFailed() throws Exception {
    assertThat(closeCodeAfter(jwtHello("not.a.jwt", "d1"))).isEqualTo(4001);
  }

  @Test
  void helloWithoutCredentialAuthFailed() throws Exception {
    assertThat(closeCodeAfter(Hello.newBuilder().setDeviceId("d1").build())).isEqualTo(4001);
  }

  @Test
  void twentyFirstHandshakePerMinuteRateLimited() throws Exception {
    for (int i = 0; i < 20; i++) {
      limiter.tryAcquire("t1:u1");
    }

    f.channel.writeInbound(bytes(hello(jwtHello(auth.jwt("t1:u1", "d1", f.now()), "d1"))));
    List<Frame> out = outbound();

    ErrorInfo e = ErrorInfo.parseFrom(out.get(0).getPayload());
    assertThat(e.getCode()).isEqualTo(4009);
    assertThat(e.getRetryAfterMs()).isEqualTo(30_000);
  }

  @Test
  void unknownKidRefreshesThenConnects() throws Exception {
    ECKey rotated = AuthFixture.newKey();
    auth.published.add(rotated);

    Connected c = send(jwtHello(auth.jwt(rotated, "t1:u1", "d1", f.now()), "d1"));

    assertThat(ConnId.isValid(c.getConnId())).isTrue();
  }

  @Test
  void unknownKidStillMissingAfterRefreshAuthFailed() throws Exception {
    ECKey unpublished = AuthFixture.newKey();

    assertThat(closeCodeAfter(jwtHello(auth.jwt(unpublished, "t1:u1", "d1", f.now()), "d1"))).isEqualTo(4001);
  }

  @Test
  void pipelinedFramesAfterHelloAreProcessedAfterConnected() throws Exception {
    ECKey rotated = AuthFixture.newKey();
    auth.published.add(rotated);
    ByteBuf hello = bytes(hello(jwtHello(auth.jwt(rotated, "t1:u1", "d1", f.now()), "d1")));
    ByteBuf ping = bytes(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PING).setMsgId("p1").build());

    f.channel.writeInbound(hello, ping);
    List<Frame> out = outbound();

    assertThat(out).extracting(Frame::getType)
        .containsExactly(FrameType.FRAME_TYPE_CONNECTED, FrameType.FRAME_TYPE_PONG);
    assertThat(out.get(1).getMsgId()).isEqualTo("p1");
  }

  @Test
  void handshakeCompleteDropsNettyProtocolHandlerAndHandshaker() {
    f.channel.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.WS);
    f.channel.pipeline().addFirst("ws",
        new io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler("/v1/ws", "pangate.v1"));
    io.netty.util.AttributeKey<Object> handshakerKey = io.netty.util.AttributeKey.valueOf(
        io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker.class, "HANDSHAKER");
    f.channel.attr(handshakerKey).set(new Object());

    f.channel.pipeline().fireUserEventTriggered(
        new io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler.HandshakeComplete(
            "/v1/ws", new io.netty.handler.codec.http.DefaultHttpHeaders(), "pangate.v1"));

    assertThat(f.channel.pipeline().get(io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler.class))
        .isNull();
    assertThat(f.channel.hasAttr(handshakerKey)).isFalse();
  }

  @Test
  void identityStringsAreCanonicalizedAcrossSessions() throws Exception {
    Connected c = send(Hello.newBuilder().setJwt(auth.jwt("t1:u1", "d1", f.now())).setDeviceId("d1")
        .setSdkVersion(new String("js-1.2.3")).setPlatform(new String("web")).build());
    LoopFixture g = new LoopFixture();
    AuthFixture a2 = new AuthFixture(g::now);
    g.channel.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.TCP);
    g.channel.attr(ChannelAttrs.PROTOCOL_READY).set(true);
    Session other = g.tracker.admit(g.channel, g.loop, InetAddress.getLoopbackAddress());
    g.channel.pipeline().addLast(new FrameCodec(g.limits),
        new HelloAuthHandler(a2.tickets, a2.flow, g.tracker, new PrincipalHandshakeLimiter(20, () -> g.now() * 1000),
            g.limits, "cell-1", g.metrics), new ErrorTerminator());
    g.channel.writeInbound(bytes(hello(Hello.newBuilder().setJwt(a2.jwt("t1:u2", "d2", g.now())).setDeviceId("d2")
        .setSdkVersion(new String("js-1.2.3")).setPlatform(new String("web")).build())));
    g.channel.runPendingTasks();

    assertThat(c.getConnId()).isNotEmpty();
    assertThat(other.identity().sdkVersion()).isSameAs(session.identity().sdkVersion());
    assertThat(other.identity().platform()).isSameAs(session.identity().platform());
    assertThat(other.identity().tenantId()).isSameAs(session.identity().tenantId());
    assertThat(session.identity().capabilities()).isEmpty();
  }

  @Test
  void upgradeEventMarksProtocolReadyAndStartsHelloDeadline() {
    f.channel.attr(ChannelAttrs.PROTOCOL_READY).set(false);
    f.channel.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.WS);

    f.channel.pipeline().fireUserEventTriggered(
        new io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler.HandshakeComplete(
            "/v1/ws", new io.netty.handler.codec.http.DefaultHttpHeaders(), "pangate.v1"));

    assertThat(f.channel.attr(ChannelAttrs.PROTOCOL_READY).get()).isTrue();
    assertThat(session.nextDeadlineSec()).isEqualTo(f.now() + 5);
  }
}
