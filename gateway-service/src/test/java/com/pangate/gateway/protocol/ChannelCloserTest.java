package com.pangate.gateway.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.ErrorInfo;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.junit.jupiter.api.Test;

class ChannelCloserTest {

  private static EmbeddedChannel ready(ChannelAttrs.TransportKind kind) {
    EmbeddedChannel ch = new EmbeddedChannel();
    ch.attr(ChannelAttrs.TRANSPORT).set(kind);
    ch.attr(ChannelAttrs.PROTOCOL_READY).set(true);
    return ch;
  }

  @Test
  void wsCloseCarriesCodeAndRetryAfter() {
    EmbeddedChannel ch = ready(ChannelAttrs.TransportKind.WS);

    ChannelCloser.close(ch, CloseCode.RATE_LIMITED, "slow down", 30_000);

    CloseWebSocketFrame f = ch.readOutbound();
    assertThat(f.statusCode()).isEqualTo(4009);
    assertThat(f.reasonText()).isEqualTo("slow down retryAfter=30000");
    f.release();
    assertThat(ch.isOpen()).isFalse();
  }

  @Test
  void tcpCloseSendsFatalErrorFrame() throws Exception {
    EmbeddedChannel ch = ready(ChannelAttrs.TransportKind.TCP);

    ChannelCloser.close(ch, CloseCode.OVERLOADED, "OVERLOADED", 15_000);

    Frame f = ch.readOutbound();
    assertThat(f.getType()).isEqualTo(FrameType.FRAME_TYPE_ERROR);
    ErrorInfo e = ErrorInfo.parseFrom(f.getPayload());
    assertThat(e.getCode()).isEqualTo(4013);
    assertThat(e.getFatal()).isTrue();
    assertThat(e.getRetryAfterMs()).isEqualTo(15_000);
    assertThat(e.getRetryable()).isTrue();
    assertThat(ch.isOpen()).isFalse();
  }

  @Test
  void notReadyChannelIsJustClosed() {
    EmbeddedChannel ch = new EmbeddedChannel();

    ChannelCloser.close(ch, CloseCode.HELLO_TIMEOUT, "HELLO_TIMEOUT", 0);

    assertThat((Object) ch.readOutbound()).isNull();
    assertThat(ch.isOpen()).isFalse();
  }

  @Test
  void closeTwiceWritesOnce() {
    EmbeddedChannel ch = ready(ChannelAttrs.TransportKind.TCP);
    ch.config().setAutoClose(false);

    ChannelCloser.close(ch, CloseCode.BAD_FRAME, "a", 0);
    ChannelCloser.close(ch, CloseCode.INTERNAL, "b", 0);

    assertThat((Object) ch.readOutbound()).isNotNull();
    assertThat((Object) ch.readOutbound()).isNull();
  }

  @Test
  void stuckCloseFrameIsForcedClosedAfterGrace() {
    // Simulates a client that stopped reading: the close frame write never completes.
    EmbeddedChannel ch = new EmbeddedChannel(new io.netty.channel.ChannelOutboundHandlerAdapter() {
      @Override
      public void write(io.netty.channel.ChannelHandlerContext ctx, Object msg, io.netty.channel.ChannelPromise p) {
        io.netty.util.ReferenceCountUtil.release(msg);
      }
    });
    ch.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.WS);
    ch.attr(ChannelAttrs.PROTOCOL_READY).set(true);

    ChannelCloser.close(ch, CloseCode.GOING_AWAY, "HEARTBEAT_TIMEOUT", 0);
    assertThat(ch.isOpen()).isTrue();

    ch.advanceTimeBy(ChannelCloser.FORCE_CLOSE_AFTER.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    ch.runScheduledPendingTasks();

    assertThat(ch.isOpen()).isFalse();
  }

  @Test
  void unwritableSlowConsumerIsClosedAtOnceWithoutQueueingACloseFrame() {
    EmbeddedChannel ch = ready(ChannelAttrs.TransportKind.WS);
    ch.config().setWriteBufferWaterMark(new io.netty.channel.WriteBufferWaterMark(8, 16));
    ch.write(io.netty.buffer.Unpooled.wrappedBuffer(new byte[64]));  // backlog the client never reads
    assertThat(ch.isWritable()).isFalse();

    ChannelCloser.close(ch, CloseCode.SLOW_CONSUMER, "SHED", 0);

    assertThat(ch.isOpen()).isFalse();
    // The backlog is dropped with the channel; nothing (no backlog, no close frame) is left to flush to the client.
    assertThat((Object) ch.readOutbound()).isNull();
  }

  @Test
  void closeCodesMatchSpec() {
    assertThat(CloseCode.BAD_FRAME.code()).isEqualTo(4000);
    assertThat(CloseCode.HELLO_TIMEOUT.code()).isEqualTo(4004);
    assertThat(CloseCode.DUPLICATE_SESSION.code()).isEqualTo(4012);
    assertThat(CloseCode.INTERNAL.code()).isEqualTo(4500);
    assertThat(CloseCode.RATE_LIMITED.defaultRetryAfterMs()).isEqualTo(30_000);
    assertThat(CloseCode.OVERLOADED.defaultRetryAfterMs()).isEqualTo(15_000);
    assertThat(CloseCode.REVOKED.retryable()).isFalse();
  }
}
