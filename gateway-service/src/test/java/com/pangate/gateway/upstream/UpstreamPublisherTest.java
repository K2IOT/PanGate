package com.pangate.gateway.upstream;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.pangate.contracts.v1.ErrorInfo;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.contracts.v1.UpstreamRecord;
import com.pangate.gateway.auth.PrincipalHandshakeLimiter;
import com.pangate.gateway.lifecycle.TestKafka;
import com.pangate.gateway.pipeline.ErrorTerminatorAccess;
import com.pangate.gateway.pipeline.SessionHandler;
import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.AuthFixture;
import com.pangate.gateway.test.LoopFixture;
import com.pangate.starter.kafka.PangateHeaders;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UpstreamPublisherTest {

  private final LoopFixture f = new LoopFixture();
  private final AuthFixture auth = new AuthFixture(f::now);
  private Session session;
  private final PrincipalHandshakeLimiter limiter = new PrincipalHandshakeLimiter(20, () -> f.now() * 1000);

  @BeforeEach
  void activeSession() {
    install(f.upstream);
  }

  private void install(UpstreamPublisher publisher) {
    f.channel.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.TCP);
    f.channel.attr(ChannelAttrs.AUTHENTICATED).set(true);
    session = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    session.activate(new Session.Identity("t1", "t1:u1", "d1", "ios", "1.0", Set.of()), "CONN", 0L,
        f.wallNow(), f.wallNow() + 3_600, 0, List.of());
    f.channel.pipeline().addLast(new SessionHandler(auth.tickets, auth.flow,
        limiter, f.policy(limiter), f.limits, "cell-1", publisher, f.delivery, f.clientTopics),
        ErrorTerminatorAccess.create());
  }

  private static Frame upstream(String type, String msgId) {
    Frame.Builder b = Frame.newBuilder().setType(FrameType.FRAME_TYPE_UPSTREAM).setMsgId(msgId)
        .setPayload(ByteString.copyFromUtf8("hi")).putHeaders("traceparent", "00-t-s-01");
    if (type != null) {
      b.putHeaders("type", type);
    }
    return b.build();
  }

  private List<Frame> send(Frame frame) {
    f.channel.writeInbound(frame);
    return drain();
  }

  private List<Frame> drain() {
    f.channel.runPendingTasks();
    List<Frame> out = new ArrayList<>();
    for (Object o; (o = f.channel.readOutbound()) != null; ) {
      out.add((Frame) o);
    }
    return out;
  }

  private static ErrorInfo error(Frame frame) throws Exception {
    assertThat(frame.getType()).isEqualTo(FrameType.FRAME_TYPE_ERROR);
    return ErrorInfo.parseFrom(frame.getPayload());
  }

  @Test
  void cmdProducesRecordAndAcksAfterBrokerAck() throws Exception {
    assertThat(send(upstream("cmd", "m1"))).isEmpty();

    ProducerRecord<String, byte[]> r = f.reliable.history().get(0);
    assertThat(r.topic()).isEqualTo("c1.gw.upstream.cmd");
    assertThat(r.key()).isEqualTo("t1:u1");
    UpstreamRecord rec = UpstreamRecord.parseFrom(r.value());
    assertThat(rec.getMsgId()).isEqualTo("m1");
    assertThat(rec.getConnId()).isEqualTo("CONN");
    assertThat(rec.getNodeId()).isEqualTo("n1");
    assertThat(rec.getCellId()).isEqualTo("cell-1");
    assertThat(rec.getTenantId()).isEqualTo("t1");
    assertThat(rec.getDeviceId()).isEqualTo("d1");
    assertThat(rec.getReceivedAtMs()).isEqualTo(f.now() * 1000);
    assertThat(rec.getHeadersMap()).containsEntry("traceparent", "00-t-s-01").doesNotContainKey("type");
    assertThat(rec.getPayload().toStringUtf8()).isEqualTo("hi");
    assertThat(PangateHeaders.read(r.headers(), "content-type"))
        .hasValue("application/x-protobuf; schema=pangate.v1.UpstreamRecord");
    assertThat(PangateHeaders.read(r.headers(), "traceparent")).hasValue("00-t-s-01");

    f.reliable.completeNext();
    List<Frame> out = drain();

    assertThat(out).singleElement().satisfies(a -> {
      assertThat(a.getType()).isEqualTo(FrameType.FRAME_TYPE_UPSTREAM_ACK);
      assertThat(a.getMsgId()).isEqualTo("m1");
    });
  }

  @Test
  void cmdFailureAnswersUnavailableAndKeepsSession() throws Exception {
    send(upstream("cmd", "m1"));

    f.reliable.errorNext(new org.apache.kafka.common.errors.TimeoutException("broker down"));
    ErrorInfo e = error(drain().get(0));

    assertThat(e.getCode()).isEqualTo(4500);
    assertThat(e.getMessage()).isEqualTo("UNAVAILABLE");
    assertThat(e.getRetryable()).isTrue();
    assertThat(e.getFatal()).isFalse();
    assertThat(f.channel.isOpen()).isTrue();
  }

  @Test
  void sendThrowingSynchronouslyIsHandled() throws Exception {
    f.channel.pipeline().remove(SessionHandler.class);
    f.channel.pipeline().remove(f.channel.pipeline().last());
    MockProducer<String, byte[]> throwing = new MockProducer<>(false, new StringSerializer(),
        new ByteArraySerializer()) {
      @Override
      public synchronized Future<RecordMetadata> send(ProducerRecord<String, byte[]> r, Callback cb) {
        throw new BufferExhaustedException("full");
      }
    };
    LoopFixture g = f;
    UpstreamPublisher p = new UpstreamPublisher(TestKafka.of(throwing, g.telemetry), g.topics, g.node, "cell-1",
        g.tenants, g.metrics, () -> 0L);
    f.channel.pipeline().addLast(new SessionHandler(auth.tickets, auth.flow,
        limiter, f.policy(limiter), f.limits, "cell-1", p, f.delivery, f.clientTopics),
        ErrorTerminatorAccess.create());

    ErrorInfo e = error(send(upstream("cmd", "m1")).get(0));

    assertThat(e.getCode()).isEqualTo(4500);
    assertThat(f.channel.isOpen()).isTrue();
  }

  @Test
  void telemetryGoesToTelemetryProducerWithoutAck() {
    assertThat(send(upstream("telemetry", "m1"))).isEmpty();

    assertThat(f.telemetry.history()).singleElement()
        .satisfies(r -> assertThat(r.topic()).isEqualTo("c1.gw.upstream.telemetry"));
    assertThat(f.reliable.history()).isEmpty();
  }

  @Test
  void telemetryDroppedWhenSwitchOff() {
    f.tenants.apply(TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
        .setSwitches(KillSwitches.newBuilder().setUpstreamTelemetryEnabled(false)).build());

    assertThat(send(upstream("telemetry", "m1"))).isEmpty();
    send(upstream("cmd", "m2"));

    assertThat(f.telemetry.history()).isEmpty();
    assertThat(f.reliable.history()).hasSize(1);
    assertThat(f.meters.get("gw.upstream").tag("type", "telemetry").tag("result", "dropped").counter().count())
        .isEqualTo(1);
  }

  @Test
  void unknownTypeAndMissingMsgIdAnswer4000NotClose() throws Exception {
    ErrorInfo unknown = error(send(upstream("bogus", "m1")).get(0));
    ErrorInfo noType = error(send(upstream(null, "m2")).get(0));
    ErrorInfo noId = error(send(upstream("cmd", "")).get(0));

    assertThat(unknown.getCode()).isEqualTo(4000);
    assertThat(unknown.getMessage()).isEqualTo("unknown type");
    assertThat(unknown.getFatal()).isFalse();
    assertThat(noType.getMessage()).isEqualTo("unknown type");
    assertThat(noId.getMessage()).isEqualTo("missing msg_id");
    assertThat(f.channel.isOpen()).isTrue();
    assertThat(f.reliable.history()).isEmpty();
  }

  @Test
  void rpcResGoesAsCmdResult() {
    send(upstream(null, "m1").toBuilder().setType(FrameType.FRAME_TYPE_RPC_RES).setCorrelationId("c1").build());

    assertThat(f.reliable.history()).singleElement()
        .satisfies(r -> assertThat(r.topic()).isEqualTo("c1.gw.upstream.cmd_result"));
  }

  @Test
  void rateLimitedAnswers4009ThenCloses() throws Exception {
    List<Frame> errors = new ArrayList<>();
    for (int i = 0; i < 101; i++) {
      errors.addAll(send(upstream("telemetry", "m" + i)));
    }
    ErrorInfo e = error(errors.get(0));
    assertThat(e.getCode()).isEqualTo(4009);
    assertThat(e.getRetryable()).isTrue();

    for (int sec = 1; sec <= 2 && f.channel.isOpen(); sec++) {
      f.clockSec.addAndGet(1);
      for (int i = 0; i < 200 && f.channel.isOpen(); i++) {
        send(upstream("telemetry", "x" + sec + "-" + i));
      }
    }
    assertThat(f.channel.isOpen()).isFalse();
    assertThat(session.closeCode().code()).isEqualTo(4009);
  }

  @Test
  void telemetryDroppedAtShedLevel2ButCmdStillProduced() {
    f.node.shedLevel(2);
    assertThat(send(upstream("telemetry", "m1"))).isEmpty();
    assertThat(f.telemetry.history()).isEmpty();
    send(upstream("cmd", "m2"));
    assertThat(f.reliable.history()).hasSize(1);
  }
}
