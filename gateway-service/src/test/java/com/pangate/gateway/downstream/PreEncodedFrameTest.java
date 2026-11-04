package com.pangate.gateway.downstream;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.junit.jupiter.api.Test;

class PreEncodedFrameTest {

  @Test
  void encodesOnceAndDuplicatesShareContent() throws Exception {
    Frame f = Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setTopic("t1:x").setMsgId("b1")
        .setClass_(DeliveryClass.DELIVERY_CLASS_EPHEMERAL).build();
    PreEncodedFrame pre = PreEncodedFrame.encode(f, "t1", 2);
    assertThat(pre.deliveryClass()).isEqualTo(DeliveryClass.DELIVERY_CLASS_EPHEMERAL);
    assertThat(pre.tenantId()).isEqualTo("t1");

    ByteBuf dup = pre.retainedDuplicate();
    assertThat(dup.isDirect()).isTrue();
    assertThat(Frame.parseFrom(ByteBufUtil.getBytes(dup))).isEqualTo(f);
    dup.release();

    assertThat(pre.refCnt()).isEqualTo(2);
    pre.releaseShare();
    pre.releaseShare();
    assertThat(pre.refCnt()).isZero();
  }
}
