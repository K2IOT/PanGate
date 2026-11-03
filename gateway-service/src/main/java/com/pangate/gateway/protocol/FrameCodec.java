package com.pangate.gateway.protocol;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.gateway.config.LimitsHolder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Protobuf envelope codec (spec §4.4). Size is checked before parsing: helloMax until authenticated, frameMax after.
 * Only client-to-gateway frame types are accepted inbound.
 */
@io.netty.channel.ChannelHandler.Sharable
public final class FrameCodec extends MessageToMessageCodec<ByteBuf, Frame> {

  private static final Set<FrameType> CLIENT_TYPES = EnumSet.of(FrameType.FRAME_TYPE_HELLO, FrameType.FRAME_TYPE_PING,
      FrameType.FRAME_TYPE_ACK, FrameType.FRAME_TYPE_UPSTREAM, FrameType.FRAME_TYPE_SUBSCRIBE,
      FrameType.FRAME_TYPE_UNSUBSCRIBE, FrameType.FRAME_TYPE_RPC_RES, FrameType.FRAME_TYPE_REAUTH);

  private final LimitsHolder limits;

  public FrameCodec(LimitsHolder limits) {
    this.limits = limits;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
    boolean authenticated = ChannelAttrs.isSet(ctx.channel(), ChannelAttrs.AUTHENTICATED);
    int max = authenticated ? limits.current().frameMax() : limits.current().helloMax();
    if (msg.readableBytes() > max) {
      throw new ProtocolException(CloseCode.BAD_FRAME, "frame too large");
    }
    Frame frame;
    try {
      frame = Frame.parseFrom(CodedInputStream.newInstance(msg.nioBuffer()));
    } catch (InvalidProtocolBufferException e) {
      throw new ProtocolException(CloseCode.BAD_FRAME, "malformed frame");
    } catch (IOException e) {
      throw new ProtocolException(CloseCode.BAD_FRAME, "malformed frame");
    }
    if (!CLIENT_TYPES.contains(frame.getType())) {
      throw new ProtocolException(CloseCode.BAD_FRAME, "unexpected frame type " + frame.getType());
    }
    out.add(frame);
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Frame msg, List<Object> out) throws IOException {
    int size = msg.getSerializedSize();
    ByteBuf buf = ctx.alloc().ioBuffer(size);
    try {
      CodedOutputStream cos = CodedOutputStream.newInstance(buf.nioBuffer(buf.writerIndex(), size));
      msg.writeTo(cos);
      cos.flush();
      buf.writerIndex(buf.writerIndex() + size);
      out.add(buf);
    } catch (IOException | RuntimeException e) {
      buf.release();
      throw e;
    }
  }
}
