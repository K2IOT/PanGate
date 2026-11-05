package com.pangate.gateway.shed;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import com.pangate.gateway.test.LoopFixture;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ShedEnforcerTest {

  private final LoopFixture f = new LoopFixture();

  private Session pending(int bytes) {
    EmbeddedChannel ch = new EmbeddedChannel();
    Session s = f.tracker.admit(ch, f.loop, InetAddress.getLoopbackAddress());
    s.activate(new Session.Identity("t1", "t1:u" + bytes, "d", "ios", "1.0", Set.of()), f.tracker.newConnId(s), 0L,
        f.wallNow(), f.wallNow() + 3600, 1, List.of());
    ch.write(Unpooled.wrappedBuffer(new byte[bytes]));
    f.loop.markDirty(s);
    return s;
  }

  @Test
  void closesLargestPendingUntilBelow90PercentOfShare() {
    Session big = pending(300);
    Session mid = pending(200);
    Session small = pending(100);
    f.loop.sampleOutbound();
    long total = big.pendingOutboundBytes() + mid.pendingOutboundBytes() + small.pendingOutboundBytes();
    // share so that dropping only the largest gets under 90 %
    long share = (long) Math.ceil((total - big.pendingOutboundBytes()) / 0.9) + 1;
    OutboundBudget budget = new OutboundBudget(share, 100L << 30, f.loops, f.metrics);

    new ShedEnforcer(f.loops, budget).relieve();
    f.channel.runPendingTasks();

    assertThat(big.state()).isEqualTo(SessionState.CLOSED);
    assertThat(big.closeCode()).isEqualTo(CloseCode.SLOW_CONSUMER);
    assertThat(big.closeReason()).isEqualTo("SHED");
    assertThat(mid.state()).isEqualTo(SessionState.ACTIVE);
    assertThat(small.state()).isEqualTo(SessionState.ACTIVE);
  }
}
