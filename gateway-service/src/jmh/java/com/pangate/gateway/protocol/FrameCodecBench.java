package com.pangate.gateway.protocol;

import com.google.protobuf.ByteString;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.gateway.test.TestLimits;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/** Spec §11: FrameCodec encode (PUSH, gateway → client) and decode (UPSTREAM, client → gateway), 256 B payload. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FrameCodecBench {

  private FrameCodec codec;
  private EmbeddedChannel channel;
  private ChannelHandlerContext ctx;
  private Frame push;
  private ByteBuf upstreamBytes;
  private final List<Object> out = new ArrayList<>(1);

  @Setup(Level.Trial)
  public void setUp() {
    codec = new FrameCodec(TestLimits.holder());
    channel = new EmbeddedChannel(codec);
    channel.attr(ChannelAttrs.AUTHENTICATED).set(true);
    ctx = channel.pipeline().context(codec);
    byte[] payload = new byte[256];
    ThreadLocalRandom.current().nextBytes(payload);
    push = Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setMsgId("01JABCDEFGHJKMNPQRSTVWXYZ0").setSeq(42)
        .setClass_(DeliveryClass.DELIVERY_CLASS_RELIABLE).setPayload(ByteString.copyFrom(payload)).build();
    Frame upstream = Frame.newBuilder().setType(FrameType.FRAME_TYPE_UPSTREAM)
        .setMsgId("01JABCDEFGHJKMNPQRSTVWXYZ0").putHeaders("type", "telemetry")
        .setPayload(ByteString.copyFrom(payload)).build();
    upstreamBytes = PooledByteBufAllocator.DEFAULT.directBuffer().writeBytes(upstream.toByteArray());
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    upstreamBytes.release();
    channel.finishAndReleaseAll();
  }

  @Benchmark
  public Object encodePush() throws Exception {
    out.clear();
    codec.encode(ctx, push, out);
    Object b = out.get(0);
    ReferenceCountUtil.release(b);
    return b;
  }

  @Benchmark
  public Object decodeUpstream() {
    out.clear();
    codec.decode(ctx, upstreamBytes.duplicate(), out);
    return out.get(0);
  }
}
