package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.LoopFixture;
import io.netty.buffer.Unpooled;
import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class WritabilityTrackerTest {

  private final LoopFixture f = new LoopFixture();

  @Test
  void anyOutboundWriteIsCountedInTheOutboundBudget() {
    Session s = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    f.channel.pipeline().addLast(WritabilityTracker.INSTANCE);

    f.channel.write(Unpooled.wrappedBuffer(new byte[100]));  // e.g. a PONG or ACK, not a push: not flushed yet
    f.loop.sampleOutbound();

    assertThat(f.loop.dirtySessions()).containsExactly(s);
    assertThat(f.loop.outboundBytes()).isGreaterThanOrEqualTo(100);
  }
}
