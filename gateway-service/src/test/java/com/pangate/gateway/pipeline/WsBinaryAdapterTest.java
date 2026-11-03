package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.protocol.ChannelAttrs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.junit.jupiter.api.Test;

class WsBinaryAdapterTest {

  private static EmbeddedChannel channel() {
    return new EmbeddedChannel(new WsBinaryAdapter(), new ErrorTerminator());
  }

  @Test
  void binaryFrameBecomesByteBuf() {
    EmbeddedChannel ch = channel();

    ch.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[] {1, 2})));

    ByteBuf in = ch.readInbound();
    assertThat(in.readableBytes()).isEqualTo(2);
    in.release();
  }

  @Test
  void textWebSocketFrameIsBadFrame() {
    EmbeddedChannel ch = channel();

    ch.writeInbound(new TextWebSocketFrame("hi"));

    assertThat(ch.isOpen()).isFalse();
  }

  @Test
  void fragmentedBinaryFrameIsBadFrame() {
    EmbeddedChannel ch = channel();

    ch.writeInbound(new BinaryWebSocketFrame(false, 0, Unpooled.wrappedBuffer(new byte[] {1})));

    assertThat(ch.isOpen()).isFalse();
  }

  @Test
  void clientCloseFrameRecordsCode() {
    EmbeddedChannel ch = channel();

    ch.writeInbound(new CloseWebSocketFrame(1000, "bye"));

    assertThat(ch.attr(ChannelAttrs.CLIENT_CLOSE_CODE).get()).isEqualTo(1000);
    assertThat(ch.isOpen()).isFalse();
  }

  @Test
  void webSocketPingIsAnsweredWithPong() {
    EmbeddedChannel ch = channel();

    ch.writeInbound(new io.netty.handler.codec.http.websocketx.PingWebSocketFrame(Unpooled.wrappedBuffer(new byte[] {9})));

    io.netty.handler.codec.http.websocketx.PongWebSocketFrame pong = ch.readOutbound();
    assertThat(pong.content().getByte(0)).isEqualTo((byte) 9);
    pong.release();
    assertThat(ch.isOpen()).isTrue();
  }

  @Test
  void adapterIsSharable() {
    assertThat(new WsBinaryAdapter().isSharable()).isTrue();
  }

  @Test
  void outboundByteBufIsWrapped() {
    EmbeddedChannel ch = channel();

    ch.writeOutbound(Unpooled.wrappedBuffer(new byte[] {7}));

    BinaryWebSocketFrame out = ch.readOutbound();
    assertThat(out.content().readableBytes()).isEqualTo(1);
    out.release();
  }
}
