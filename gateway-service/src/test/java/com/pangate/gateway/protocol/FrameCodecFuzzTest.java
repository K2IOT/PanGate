package com.pangate.gateway.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.gateway.test.LeakWatch;
import com.pangate.gateway.test.TestLimits;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.util.ResourceLeakDetector;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Spec §11: random input into {@link FrameCodec#decode}; only BAD_FRAME may come out, and no ByteBuf leaks. Limits are
 * small (HELLO 256 B, frames 1 KiB) so fuzzed inputs, at most 4 KiB, cross both size checks; the input is a pooled
 * buffer so the leak detector tracks it.
 */
class FrameCodecFuzzTest {

  private static final Set<FrameType> CLIENT_TYPES = EnumSet.of(FrameType.FRAME_TYPE_HELLO, FrameType.FRAME_TYPE_PING,
      FrameType.FRAME_TYPE_ACK, FrameType.FRAME_TYPE_UPSTREAM, FrameType.FRAME_TYPE_SUBSCRIBE,
      FrameType.FRAME_TYPE_UNSUBSCRIBE, FrameType.FRAME_TYPE_RPC_RES, FrameType.FRAME_TYPE_REAUTH);
  static final int HELLO_MAX = 256;
  static final int FRAME_MAX = 1024;
  private static final com.pangate.gateway.config.LimitsHolder LIMITS =
      new com.pangate.gateway.config.LimitsHolder(TestLimits.withFrameLimits(FRAME_MAX, HELLO_MAX));
  private static ResourceLeakDetector.Level before;

  @BeforeAll
  static void watchLeaks() {
    before = ResourceLeakDetector.getLevel();
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    LeakWatch.install();
    LeakWatch.reset();
  }

  @AfterAll
  static void noLeaks() {
    LeakWatch.collect();
    try {
      assertThat(LeakWatch.leaks("FrameCodecFuzzTest")).as("ByteBuf leaks created by this fuzz test").isZero();
    } finally {
      ResourceLeakDetector.setLevel(before);
    }
  }

  @FuzzTest(maxDuration = "10m")
  void decode(FuzzedDataProvider data) {
    boolean authenticated = data.consumeBoolean();
    List<Object> frames = new ArrayList<>();
    List<Throwable> errors = new ArrayList<>();
    EmbeddedChannel ch = new EmbeddedChannel(new FrameCodec(LIMITS), new ChannelInboundHandlerAdapter() {
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        frames.add(msg);
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        errors.add(cause);
      }
    });
    if (authenticated) {
      ch.attr(ChannelAttrs.AUTHENTICATED).set(true);
    }
    try {
      byte[] bytes = data.consumeRemainingAsBytes();
      ch.writeInbound(ch.alloc().buffer(bytes.length).writeBytes(bytes));
      for (Throwable t : errors) {
        Throwable cause = t instanceof DecoderException && t.getCause() != null ? t.getCause() : t;
        assertThat(cause).isInstanceOf(ProtocolException.class);
        assertThat(((ProtocolException) cause).code()).isEqualTo(CloseCode.BAD_FRAME);
      }
      for (Object f : frames) {
        assertThat(f).isInstanceOf(Frame.class);
        assertThat(CLIENT_TYPES).contains(((Frame) f).getType());
      }
    } finally {
      ch.finishAndReleaseAll();
    }
  }
}
