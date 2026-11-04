package com.pangate.gateway.shed;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.LoopFixture;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class OutboundBudgetTest {

  private final LoopFixture f = new LoopFixture();
  private final OutboundBudget budget = new OutboundBudget(1_000_000, 100L << 30, f.loops, f.metrics);

  private Session session(EmbeddedChannel ch) {
    return f.tracker.admit(ch, f.loop, InetAddress.getLoopbackAddress());
  }

  private void sample() {
    budget.sample();
    f.channel.runPendingTasks();
  }

  @Test
  void effectiveBudgetIsCappedAt35PercentOfDirectMemory() {
    assertThat(OutboundBudget.effective(4L << 30, 1L << 30)).isEqualTo((long) (0.35 * (1L << 30)));
    assertThat(OutboundBudget.effective(256L << 20, 4L << 30)).isEqualTo(256L << 20);
    assertThat(budget.budgetBytes()).isEqualTo(1_000_000);
    assertThat(budget.shareBytes()).isEqualTo(1_000_000);
  }

  @Test
  void sampleSumsPendingBytesOfDirtySessions() {
    EmbeddedChannel c1 = new EmbeddedChannel();
    EmbeddedChannel c2 = new EmbeddedChannel();
    Session s1 = session(c1);
    Session s2 = session(c2);
    c1.write(Unpooled.wrappedBuffer(new byte[1000]));
    c2.write(Unpooled.wrappedBuffer(new byte[500]));
    f.loop.markDirty(s1);
    f.loop.markDirty(s2);
    f.loop.markDirty(s1);  // once only

    sample();
    sample();

    // Netty counts each queued entry with its bookkeeping overhead: compare with what the channels report.
    long expected = s1.pendingOutboundBytes() + s2.pendingOutboundBytes();
    assertThat(expected).isGreaterThanOrEqualTo(1500);
    assertThat(f.loop.outboundBytes()).isEqualTo(expected);
    assertThat(budget.ratio()).isEqualTo(expected / 1_000_000.0);
    assertThat(f.meters.get("gw.pending.outbound.bytes").gauge().value()).isEqualTo(expected);

    c1.flush();
    c1.releaseOutbound();
    sample();
    sample();
    assertThat(f.loop.outboundBytes()).isEqualTo(s2.pendingOutboundBytes());
    assertThat(s1.dirty()).isFalse();
    assertThat(s2.dirty()).isTrue();
    c2.releaseOutbound();
  }
}
