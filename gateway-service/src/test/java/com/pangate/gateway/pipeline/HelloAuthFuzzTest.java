package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Hello;
import com.pangate.gateway.auth.PrincipalHandshakeLimiter;
import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.FrameCodec;
import com.pangate.gateway.registry.SeqReader;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import com.pangate.gateway.test.AuthFixture;
import com.pangate.gateway.test.LeakWatch;
import com.pangate.gateway.test.LoopFixture;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.ResourceLeakDetector;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Spec §11: random HELLOs through FrameCodec → HelloAuthHandler → SessionHandler. Every input ends either with the
 * channel closed or with an ACTIVE session that got exactly one CONNECTED; nothing escapes the pipeline, nothing leaks.
 */
class HelloAuthFuzzTest {

  private static final long NOW = new LoopFixture().now();
  private static AuthFixture auth;
  private static String validJwt;
  private static ResourceLeakDetector.Level before;

  private static final SeqReader NO_SEQ = new SeqReader() {
    @Override
    public CompletionStage<OptionalLong> read(String principal) {
      return CompletableFuture.completedFuture(OptionalLong.empty());
    }

    @Override
    public int seqEpoch(String principal) {
      return 0;
    }
  };

  @BeforeAll
  static void setUp() {
    before = ResourceLeakDetector.getLevel();
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    LeakWatch.install();
    LeakWatch.reset();
    auth = new AuthFixture(() -> NOW);
    validJwt = auth.jwt("t1:u1", "d1", NOW);
  }

  @AfterAll
  static void noLeaks() {
    LeakWatch.collect();
    try {
      assertThat(LeakWatch.leaks("HelloAuthFuzzTest")).as("ByteBuf leaks created by this fuzz test").isZero();
    } finally {
      ResourceLeakDetector.setLevel(before);
    }
  }

  @FuzzTest(maxDuration = "10m")
  void hello(FuzzedDataProvider data) {
    LoopFixture f = new LoopFixture();
    PrincipalHandshakeLimiter limiter = new PrincipalHandshakeLimiter(20, () -> f.now() * 1000);
    f.channel.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.TCP);
    f.channel.attr(ChannelAttrs.PROTOCOL_READY).set(true);
    Session session = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    f.channel.pipeline().addLast(new FrameCodec(f.limits),
        new HelloAuthHandler(auth.tickets, auth.flow, f.tracker, f.policy(limiter), f.tenants, f.limits, "cell-1",
            f.metrics, NO_SEQ),
        new SessionHandler(auth.tickets, auth.flow, limiter, f.policy(limiter), f.limits, "cell-1", f.upstream,
            f.delivery, f.clientTopics),
        new ErrorTerminator());
    try {
      f.channel.writeInbound(Unpooled.wrappedBuffer(input(data)));
      f.channel.runPendingTasks();
      f.channel.checkException();
      int connected = 0;
      for (Object o; (o = f.channel.readOutbound()) != null; ) {
        ByteBuf b = (ByteBuf) o;
        try {
          if (Frame.parseFrom(ByteBufUtil.getBytes(b)).getType() == FrameType.FRAME_TYPE_CONNECTED) {
            connected++;
          }
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          throw new AssertionError("gateway wrote an unparsable frame", e);
        } finally {
          b.release();
        }
      }
      if (f.channel.isOpen()) {
        assertThat(session.state()).as("open channel after HELLO").isEqualTo(SessionState.ACTIVE);
        assertThat(connected).as("CONNECTED frames on an open channel").isEqualTo(1);
      } else {
        assertThat(connected).as("CONNECTED frames on a closed channel").isLessThanOrEqualTo(1);
      }
    } finally {
      f.channel.finishAndReleaseAll();
    }
  }

  /** mode 0 raw bytes; 1 random JWT; 2 valid JWT with one byte flipped; 3 random resume ticket. */
  private static byte[] input(FuzzedDataProvider data) {
    int mode = data.consumeInt(0, 3);
    if (mode == 0) {
      return data.consumeRemainingAsBytes();
    }
    Hello.Builder h = Hello.newBuilder();
    if (mode == 1) {
      h.setJwt(data.consumeString(2048)).setDeviceId("d1");
    } else if (mode == 2) {
      byte[] jwt = validJwt.getBytes(StandardCharsets.US_ASCII);
      int at = data.consumeInt(0, jwt.length - 1);
      jwt[at] = data.consumeByte();
      h.setJwt(new String(jwt, StandardCharsets.ISO_8859_1)).setDeviceId("d1");
    } else {
      h.setResumeTicket(data.consumeString(2048)).setDeviceId(data.consumeString(64))
          .setSdkVersion(data.consumeString(32)).setPlatform(data.consumeString(16));
    }
    return Frame.newBuilder().setType(FrameType.FRAME_TYPE_HELLO).setPayload(h.build().toByteString()).build()
        .toByteArray();
  }
}
