package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.Connected;
import com.pangate.contracts.v1.ErrorInfo;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Hello;
import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.protocol.FrameCodec;
import com.pangate.gateway.registry.SeqReader;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.AuthFixture;
import com.pangate.gateway.test.LoopFixture;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SseAdapterTest {

  private final LoopFixture f = new LoopFixture();
  private final AuthFixture auth = new AuthFixture(f::wallNow);
  private final com.pangate.gateway.auth.PrincipalHandshakeLimiter limiter =
      new com.pangate.gateway.auth.PrincipalHandshakeLimiter(20, () -> f.now() * 1000);
  private final AtomicInteger streams = new AtomicInteger();
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
  void pipeline() {
    session = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    f.channel.pipeline().addLast(new SseAdapter(Duration.ofSeconds(30), streams, f.limits), new FrameCodec(f.limits),
        new HelloAuthHandler(auth.tickets, auth.flow, f.tracker, f.policy(limiter), f.tenants, f.limits, "cell-1",
            f.metrics, seq),
        new SessionHandler(auth.tickets, auth.flow, limiter, f.policy(limiter), f.limits, "cell-1", f.upstream,
            f.delivery, f.clientTopics), new ErrorTerminator());
  }

  private void enableSse() {
    f.tenants.apply(TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
        .setSwitches(KillSwitches.newBuilder().setSseFallbackEnabled(true)).build());
  }

  private FullHttpRequest post(HttpMethod method, String contentType, byte[] body) {
    FullHttpRequest r = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, "/v1/sse",
        Unpooled.wrappedBuffer(body));
    r.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType).set(HttpHeaderNames.ORIGIN, "https://app.example");
    return r;
  }

  private byte[] hello() {
    return Hello.newBuilder().setJwt(auth.jwt("t1:u1", "d1", f.wallNow())).setDeviceId("d1").setSdkVersion("1.0")
        .build().toByteArray();
  }

  private List<Object> outbound() {
    f.channel.runPendingTasks();
    List<Object> out = new ArrayList<>();
    for (Object o; (o = f.channel.readOutbound()) != null; ) {
      out.add(o);
    }
    return out;
  }

  private static String text(Object content) {
    String s = ((HttpContent) content).content().toString(StandardCharsets.US_ASCII);
    ReferenceCountUtil.release(content);
    return s;
  }

  private static Frame event(Object content) throws Exception {
    String s = text(content);
    assertThat(s).startsWith("data: ").endsWith("\n\n");
    return Frame.parseFrom(Base64.getDecoder().decode(s.substring(6, s.length() - 2)));
  }

  @Test
  void helloInBodyStreamsConnectedAsFirstEvent() throws Exception {
    enableSse();
    f.channel.writeInbound(post(HttpMethod.POST, "application/x-protobuf", hello()));
    List<Object> out = outbound();

    HttpResponse head = (HttpResponse) out.get(0);
    assertThat(head).isNotInstanceOf(FullHttpResponse.class);
    assertThat(head.status().code()).isEqualTo(200);
    assertThat(head.headers().get(HttpHeaderNames.CONTENT_TYPE)).isEqualTo("text/event-stream");
    assertThat(head.headers().get(HttpHeaderNames.CACHE_CONTROL)).isEqualTo("no-cache");
    Frame connected = event(out.get(1));
    assertThat(connected.getType()).isEqualTo(FrameType.FRAME_TYPE_CONNECTED);
    assertThat(Connected.parseFrom(connected.getPayload()).getResumeTicket()).isNotEmpty();
    assertThat(f.channel.attr(ChannelAttrs.TRANSPORT).get()).isEqualTo(ChannelAttrs.TransportKind.SSE);
    assertThat(session.origin()).isEqualTo("https://app.example");
    assertThat(streams.get()).isEqualTo(1);
    f.channel.close();
    f.channel.runPendingTasks();
    assertThat(streams.get()).isZero();
  }

  @Test
  void tenantWithoutSseGets401() {
    f.channel.writeInbound(post(HttpMethod.POST, "application/x-protobuf", hello()));
    List<Object> out = outbound();

    FullHttpResponse r = (FullHttpResponse) out.get(0);
    assertThat(r.status().code()).isEqualTo(401);
    r.release();
    assertThat(f.channel.isOpen()).isFalse();
    assertThat(f.meters.counter("gw.handshakes", "result", "sse_disabled").count()).isEqualTo(1);
  }

  @Test
  void wrongMethodIs405() {
    f.channel.writeInbound(post(HttpMethod.GET, "application/x-protobuf", new byte[0]));
    FullHttpResponse r = (FullHttpResponse) outbound().get(0);
    assertThat(r.status().code()).isEqualTo(405);
    r.release();
  }

  @Test
  void wrongContentTypeIs415() {
    f.channel.writeInbound(post(HttpMethod.POST, "text/plain", hello()));
    FullHttpResponse r = (FullHttpResponse) outbound().get(0);
    assertThat(r.status().code()).isEqualTo(415);
    r.release();
  }

  @Test
  void heartbeatCommentTouchesSession() throws Exception {
    enableSse();
    f.channel.writeInbound(post(HttpMethod.POST, "application/x-protobuf", hello()));
    outbound().forEach(ReferenceCountUtil::release);

    f.clockSec.addAndGet(30);
    f.channel.advanceTimeBy(30, TimeUnit.SECONDS);
    f.channel.runScheduledPendingTasks();
    List<Object> out = outbound();

    assertThat(out).hasSize(1);
    assertThat(text(out.get(0))).isEqualTo(":\n\n");
    assertThat(session.lastSeenSec()).isEqualTo(f.loop.nowSec());
  }

  @Test
  void closeWhileStreamingSendsErrorEventAndLastChunk() throws Exception {
    enableSse();
    f.channel.writeInbound(post(HttpMethod.POST, "application/x-protobuf", hello()));
    outbound().forEach(ReferenceCountUtil::release);

    session.close(CloseCode.REVOKED, "REVOKED");
    List<Object> out = outbound();

    Frame err = event(out.get(0));
    assertThat(err.getType()).isEqualTo(FrameType.FRAME_TYPE_ERROR);
    ErrorInfo e = ErrorInfo.parseFrom(err.getPayload());
    assertThat(e.getCode()).isEqualTo(4003);
    assertThat(e.getFatal()).isTrue();
    assertThat(out.get(1)).isInstanceOf(LastHttpContent.class);
    assertThat(f.channel.isOpen()).isFalse();
  }

  @Test
  void authFailureBeforeStreamingIsHttp401() {
    enableSse();
    byte[] bad = Hello.newBuilder().setJwt("not-a-jwt").setDeviceId("d1").build().toByteArray();
    f.channel.writeInbound(post(HttpMethod.POST, "application/x-protobuf", bad));
    FullHttpResponse r = (FullHttpResponse) outbound().get(0);
    assertThat(r.status().code()).isEqualTo(401);
    r.release();
  }

  @Test
  void httpStatusMapping() {
    assertThat(SseAdapter.httpStatus(CloseCode.AUTH_FAILED).code()).isEqualTo(401);
    assertThat(SseAdapter.httpStatus(CloseCode.SESSION_EXPIRED).code()).isEqualTo(401);
    assertThat(SseAdapter.httpStatus(CloseCode.REVOKED).code()).isEqualTo(403);
    assertThat(SseAdapter.httpStatus(CloseCode.RATE_LIMITED).code()).isEqualTo(429);
    assertThat(SseAdapter.httpStatus(CloseCode.WRONG_CELL).code()).isEqualTo(421);
    assertThat(SseAdapter.httpStatus(CloseCode.OVERLOADED).code()).isEqualTo(503);
    assertThat(SseAdapter.httpStatus(CloseCode.BAD_FRAME).code()).isEqualTo(400);
    assertThat(SseAdapter.httpStatus(CloseCode.GOING_AWAY).code()).isEqualTo(503);  // drain: retry elsewhere
    assertThat(SseAdapter.httpStatus(CloseCode.HELLO_TIMEOUT).code()).isEqualTo(503);
    assertThat(SseAdapter.httpStatus(CloseCode.INTERNAL).code()).isEqualTo(503);
  }

  @Test
  void heartbeatFollowsAShortTenantHeartbeat() {
    f.tenants.apply(TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
        .setSwitches(KillSwitches.newBuilder().setSseFallbackEnabled(true).setHeartbeatIntervalSec(5)).build());
    f.channel.writeInbound(post(HttpMethod.POST, "application/x-protobuf", hello()));
    outbound().forEach(ReferenceCountUtil::release);

    f.channel.advanceTimeBy(5, TimeUnit.SECONDS);
    f.channel.runScheduledPendingTasks();
    List<Object> out = outbound();
    assertThat(out).hasSize(1);
    assertThat(text(out.get(0))).isEqualTo(":\n\n");
  }

  @Test
  void allowedOriginIsEchoedOnTheStream() {
    f.tenants.apply(TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
        .setSwitches(KillSwitches.newBuilder().setSseFallbackEnabled(true))
        .setLimits(com.pangate.contracts.v1.TenantLimits.newBuilder().addOriginAllowlist("https://app.example"))
        .build());
    f.channel.writeInbound(post(HttpMethod.POST, "application/x-protobuf", hello()));
    List<Object> out = outbound();
    HttpResponse head = (HttpResponse) out.get(0);
    assertThat(head.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN)).isEqualTo("https://app.example");
    assertThat(head.headers().get(HttpHeaderNames.VARY)).isEqualTo("Origin");
    out.forEach(ReferenceCountUtil::release);
  }

  @Test
  void sseSocketsGetATcpUserTimeoutOnEpoll() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(io.netty.channel.epoll.Epoll.isAvailable());
    io.netty.channel.epoll.EpollSocketChannel ch = new io.netty.channel.epoll.EpollSocketChannel();
    try {
      SseAdapter.applyUserTimeout(ch, Duration.ofSeconds(90));
      assertThat(ch.config().getOption(io.netty.channel.epoll.EpollChannelOption.TCP_USER_TIMEOUT))
          .isEqualTo(90_000);
    } finally {
      ch.unsafe().closeForcibly();
    }
  }
}
