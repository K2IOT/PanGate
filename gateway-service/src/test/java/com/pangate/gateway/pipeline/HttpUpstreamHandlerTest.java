package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.gateway.auth.ResumeTicket;
import com.pangate.gateway.auth.ResumeTicketService;
import com.pangate.gateway.test.AuthFixture;
import com.pangate.gateway.test.LoopFixture;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpUpstreamHandlerTest {

  private final LoopFixture f = new LoopFixture();
  private final AuthFixture auth = new AuthFixture(f::wallNow);
  private final com.pangate.gateway.auth.PrincipalHandshakeLimiter limiter =
      new com.pangate.gateway.auth.PrincipalHandshakeLimiter(20, () -> f.now() * 1000);

  @BeforeEach
  void pipeline() {
    f.channel.pipeline().addLast(new HttpUpstreamHandler(auth.tickets, f.policy(limiter), f.upstream, f.limits,
        f.metrics, f::wallNow));
  }

  private String ticket(String cell) {
    long now = f.wallNow();
    return auth.tickets.issue(new ResumeTicket(cell, "t1", "t1:u1", "d1", now, now + 3600,
        ResumeTicketService.jtiHash("j1"), List.of()));
  }

  private static Frame upstream(String type, String msgId) {
    Frame.Builder b = Frame.newBuilder().setType(FrameType.FRAME_TYPE_UPSTREAM).setPayload(ByteString.copyFromUtf8("x"));
    if (msgId != null) {
      b.setMsgId(msgId);
    }
    if (type != null) {
      b.putHeaders("type", type);
    }
    return b.build();
  }

  private static FullHttpRequest post(String auth, byte[] body) {
    FullHttpRequest r = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/v1/upstream",
        Unpooled.wrappedBuffer(body));
    r.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/x-protobuf")
        .setInt(HttpHeaderNames.CONTENT_LENGTH, body.length);
    if (auth != null) {
      r.headers().set(HttpHeaderNames.AUTHORIZATION, auth);
    }
    return r;
  }

  private int status(FullHttpRequest r) {
    f.channel.writeInbound(r);
    f.channel.runPendingTasks();
    FullHttpResponse res = f.channel.readOutbound();
    assertThat(res).as("response").isNotNull();
    int code = res.status().code();
    assertThat(res.headers().get(HttpHeaderNames.CONNECTION)).isEqualToIgnoringCase(HttpHeaderValues.CLOSE.toString());
    res.release();
    return code;
  }

  @Test
  void validCmdIs202AfterKafkaAck() {
    f.channel.writeInbound(post("Ticket " + ticket("cell-1"), upstream("cmd", "m1").toByteArray()));
    f.channel.runPendingTasks();
    assertThat((Object) f.channel.readOutbound()).isNull();  // waits for the broker
    assertThat(f.reliable.history()).hasSize(1);

    f.reliable.completeNext();
    f.channel.runPendingTasks();
    FullHttpResponse res = f.channel.readOutbound();
    assertThat(res.status().code()).isEqualTo(202);
    res.release();
    assertThat(f.channel.isOpen()).isFalse();
    assertThat(f.meters.counter("gw.http.upstream", "status", "202").count()).isEqualTo(1);
  }

  @Test
  void telemetryIs202AtOnce() {
    assertThat(status(post("Ticket " + ticket("cell-1"), upstream("telemetry", "m1").toByteArray()))).isEqualTo(202);
  }

  @Test
  void kafkaFailureIs503() {
    f.channel.writeInbound(post("Ticket " + ticket("cell-1"), upstream("cmd", "m1").toByteArray()));
    f.channel.runPendingTasks();
    f.reliable.errorNext(new org.apache.kafka.common.errors.TimeoutException("broker down"));
    f.channel.runPendingTasks();
    FullHttpResponse res = f.channel.readOutbound();
    assertThat(res.status().code()).isEqualTo(503);
    res.release();
  }

  @Test
  void expiredOrForgedTicketIs401() {
    long now = f.wallNow();
    String expired = auth.tickets.issue(new ResumeTicket("cell-1", "t1", "t1:u1", "d1", now - 7200, now - 3600, 1L,
        List.of()));
    assertThat(status(post("Ticket " + expired, upstream("cmd", "m1").toByteArray()))).isEqualTo(401);
  }

  @Test
  void forgedTicketIs401() {
    assertThat(status(post("Ticket AAAA", upstream("cmd", "m1").toByteArray()))).isEqualTo(401);
  }

  @Test
  void missingAuthorizationIs401() {
    assertThat(status(post(null, upstream("cmd", "m1").toByteArray()))).isEqualTo(401);
  }

  @Test
  void wrongCellIs403() {
    assertThat(status(post("Ticket " + ticket("cell-9"), upstream("cmd", "m1").toByteArray()))).isEqualTo(403);
  }

  @Test
  void blockedTenantIs429WithRetryAfter300() {
    f.tenants.apply(TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
        .setSwitches(KillSwitches.newBuilder().setTenantBlocked(true)).build());
    f.channel.writeInbound(post("Ticket " + ticket("cell-1"), upstream("cmd", "m1").toByteArray()));
    f.channel.runPendingTasks();
    FullHttpResponse res = f.channel.readOutbound();
    assertThat(res.status().code()).isEqualTo(429);
    assertThat(res.headers().get(HttpHeaderNames.RETRY_AFTER)).isEqualTo("300");
    res.release();
  }

  @Test
  void unknownTypeOrMissingMsgIdIs400() {
    assertThat(status(post("Ticket " + ticket("cell-1"), upstream("nope", "m1").toByteArray()))).isEqualTo(400);
  }

  @Test
  void missingMsgIdIs400() {
    assertThat(status(post("Ticket " + ticket("cell-1"), upstream("cmd", null).toByteArray()))).isEqualTo(400);
  }

  @Test
  void nonUpstreamFrameIs400() {
    byte[] ping = Frame.newBuilder().setType(FrameType.FRAME_TYPE_PING).build().toByteArray();
    assertThat(status(post("Ticket " + ticket("cell-1"), ping))).isEqualTo(400);
  }

  @Test
  void malformedBodyIs400() {
    assertThat(status(post("Ticket " + ticket("cell-1"), new byte[] {(byte) 0xFF, 0x01}))).isEqualTo(400);
  }

  @Test
  void getIs405() {
    FullHttpRequest r = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/upstream");
    assertThat(status(r)).isEqualTo(405);
  }

  @Test
  void wrongContentTypeIs415() {
    FullHttpRequest r = post("Ticket " + ticket("cell-1"), upstream("cmd", "m1").toByteArray());
    r.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
    assertThat(status(r)).isEqualTo(415);
  }

  @Test
  void oversizeIs413() {
    assertThat(status(post("Ticket " + ticket("cell-1"), new byte[65_537]))).isEqualTo(413);
  }
}
