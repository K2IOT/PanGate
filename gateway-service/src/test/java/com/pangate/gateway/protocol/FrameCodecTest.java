package com.pangate.gateway.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.gateway.pipeline.ErrorTerminator;
import com.pangate.gateway.test.TestLimits;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

class FrameCodecTest {

  private static EmbeddedChannel channel(boolean authenticated) {
    EmbeddedChannel ch = new EmbeddedChannel(new FrameCodec(TestLimits.holder()), new ErrorTerminator());
    ch.attr(ChannelAttrs.AUTHENTICATED).set(authenticated);
    return ch;
  }

  private static ByteBuf bytes(Frame f) {
    return Unpooled.wrappedBuffer(f.toByteArray());
  }

  private static Frame sized(FrameType type, int totalSize) {
    Frame f = Frame.newBuilder().setType(type).setPayload(ByteString.copyFrom(new byte[totalSize])).build();
    int overhead = f.getSerializedSize() - totalSize;
    return Frame.newBuilder().setType(type).setPayload(ByteString.copyFrom(new byte[totalSize - overhead])).build();
  }

  @Test
  void decodesValidFrame() {
    EmbeddedChannel ch = channel(false);

    ch.writeInbound(bytes(Frame.newBuilder().setType(FrameType.FRAME_TYPE_HELLO).build()));

    assertThat(((Frame) ch.readInbound()).getType()).isEqualTo(FrameType.FRAME_TYPE_HELLO);
    assertThat(ch.isOpen()).isTrue();
  }

  @Test
  void rejectsHelloLargerThan8192BeforeAuth() {
    EmbeddedChannel ch = channel(false);
    Frame f = sized(FrameType.FRAME_TYPE_HELLO, 8193);
    assertThat(f.getSerializedSize()).isEqualTo(8193);

    ch.writeInbound(bytes(f));

    assertThat(ch.isOpen()).isFalse();
    assertThat((Object) ch.readOutbound()).isNull();
  }

  @Test
  void accepts8192BeforeAuth() {
    EmbeddedChannel ch = channel(false);

    ch.writeInbound(bytes(sized(FrameType.FRAME_TYPE_HELLO, 8192)));

    assertThat(ch.isOpen()).isTrue();
  }

  @Test
  void allows65536AfterAuthRejects65537() {
    EmbeddedChannel ok = channel(true);
    ok.writeInbound(bytes(sized(FrameType.FRAME_TYPE_UPSTREAM, 65_536)));
    assertThat(ok.isOpen()).isTrue();

    EmbeddedChannel tooBig = channel(true);
    tooBig.writeInbound(bytes(sized(FrameType.FRAME_TYPE_UPSTREAM, 65_537)));
    assertThat(tooBig.isOpen()).isFalse();
  }

  @Test
  void rejectsGarbageAsBadFrame() {
    EmbeddedChannel ch = channel(false);

    ch.writeInbound(Unpooled.wrappedBuffer(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}));

    assertThat(ch.isOpen()).isFalse();
  }

  @Test
  void rejectsServerOnlyTypeFromClient() {
    EmbeddedChannel ch = channel(true);

    ch.writeInbound(bytes(Frame.newBuilder().setType(FrameType.FRAME_TYPE_CONNECTED).build()));

    assertThat(ch.isOpen()).isFalse();
  }

  @Test
  void encodesFrameToBytes() throws Exception {
    EmbeddedChannel ch = channel(true);
    Frame f = Frame.newBuilder().setType(FrameType.FRAME_TYPE_PONG).setMsgId("m1").build();

    ch.writeOutbound(f);

    ByteBuf out = ch.readOutbound();
    assertThat(Frame.parseFrom(ByteBufUtil.getBytes(out))).isEqualTo(f);
    out.release();
  }

  private static java.util.List<String> traceLines(Runnable r) {
    ch.qos.logback.classic.Logger log = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("pangate.trace");
    ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> lines =
        new ch.qos.logback.core.read.ListAppender<>();
    lines.start();
    log.addAppender(lines);
    try {
      r.run();
    } finally {
      log.detachAppender(lines);
    }
    return lines.list.stream().map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage).toList();
  }

  private static EmbeddedChannel sessionChannel(boolean traced) {
    com.pangate.gateway.test.LoopFixture f = new com.pangate.gateway.test.LoopFixture();
    EmbeddedChannel ch = channel(true);
    com.pangate.gateway.session.Session s = f.tracker.admit(ch, f.loop, java.net.InetAddress.getLoopbackAddress());
    s.activate(new com.pangate.gateway.session.Session.Identity("t1", "t1:u1", "d1", "ios", "1.0",
        java.util.Set.of()), "CONN", 0L, f.wallNow(), f.wallNow() + 3600, 1, java.util.List.of());
    s.traced(traced);
    return ch;
  }

  @Test
  void tracedSessionLogsFramesWithHashedPrincipalOnly() {
    EmbeddedChannel ch = sessionChannel(true);
    java.util.List<String> lines = traceLines(() -> {
      ch.writeInbound(io.netty.buffer.Unpooled.wrappedBuffer(
          Frame.newBuilder().setType(FrameType.FRAME_TYPE_PING).build().toByteArray()));
      ch.writeOutbound(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PONG).build());
    });
    assertThat(lines).hasSize(2);
    assertThat(lines.get(0)).contains("in PING");
    assertThat(lines.get(1)).contains("out PONG");
    ch.finishAndReleaseAll();
    assertThat(lines).noneMatch(l -> l.contains("t1:u1"));
  }

  @Test
  void untracedSessionLogsNothing() {
    EmbeddedChannel ch = sessionChannel(false);
    assertThat(traceLines(() -> ch.writeInbound(io.netty.buffer.Unpooled.wrappedBuffer(
        Frame.newBuilder().setType(FrameType.FRAME_TYPE_PING).build().toByteArray())))).isEmpty();
  }
}
