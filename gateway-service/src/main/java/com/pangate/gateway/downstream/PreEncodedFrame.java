package com.pangate.gateway.downstream;

import com.google.protobuf.CodedOutputStream;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A broadcast frame encoded once into a direct buffer and shared by every loop (spec §6.3, §6.4): refCnt = number of
 * loops; each loop writes {@link #retainedDuplicate()} per recipient and calls {@link #releaseShare()} once.
 */
public final class PreEncodedFrame {

  private final ByteBuf buf;
  private final DeliveryClass deliveryClass;
  private final String tenantId;

  private PreEncodedFrame(ByteBuf buf, DeliveryClass deliveryClass, String tenantId) {
    this.buf = buf;
    this.deliveryClass = deliveryClass;
    this.tenantId = tenantId;
  }

  public static PreEncodedFrame encode(Frame f, String tenantId, int shares) {
    int size = f.getSerializedSize();
    ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(size, size);
    try {
      CodedOutputStream out = CodedOutputStream.newInstance(buf.nioBuffer(0, size));
      f.writeTo(out);
      out.flush();
      buf.writerIndex(size);
    } catch (IOException e) {
      buf.release();
      throw new UncheckedIOException(e);
    }
    if (shares > 1) {
      buf.retain(shares - 1);
    }
    return new PreEncodedFrame(buf, f.getClass_(), tenantId);
  }

  public ByteBuf retainedDuplicate() {
    return buf.retainedDuplicate();
  }

  public void releaseShare() {
    buf.release();
  }

  public int refCnt() {
    return buf.refCnt();
  }

  public DeliveryClass deliveryClass() {
    return deliveryClass;
  }

  public String tenantId() {
    return tenantId;
  }
}
