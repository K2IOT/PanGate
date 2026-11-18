package com.pangate.gateway.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.gateway.diagnostics.ListenerBindException;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * A bind the event loop never completes must fail startup (spec §3.7, fail fast), not hang it: a CI run once sat 28
 * minutes in a gateway start with nothing logged after the gRPC listener.
 */
class BindWaitTest {

  @Test
  @org.junit.jupiter.api.Timeout(value = 10, threadMode = org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD)
  void aBindThatNeverCompletesFailsWithinTheLimit() {
    EmbeddedChannel ch = new EmbeddedChannel();
    // an executor other than the caller's, as main waiting on a gw-io loop that never runs the bind
    ChannelPromise never = new io.netty.channel.DefaultChannelPromise(ch,
        io.netty.util.concurrent.GlobalEventExecutor.INSTANCE);
    long t0 = System.nanoTime();
    assertThatThrownBy(() -> NettyGatewayServer.awaitBound(never, Duration.ofMillis(300), "ws", 4242,
        "GATEWAY_LISTENERS_WS_PORT"))
        .isInstanceOf(ListenerBindException.class).hasMessageContaining("4242")
        .hasRootCauseMessage("bind not done within PT0.3S");
    assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(5));
    assertThat(never.isCancelled()).isTrue();
  }

  @Test
  void aCompletedBindReturnsItsChannel() {
    EmbeddedChannel ch = new EmbeddedChannel();
    assertThat(NettyGatewayServer.awaitBound(ch.newSucceededFuture(), Duration.ofSeconds(1), "ws", 1, "X"))
        .isSameAs(ch);
  }

  @Test
  void aFailedBindIsAListenerBindException() {
    EmbeddedChannel ch = new EmbeddedChannel();
    assertThatThrownBy(() -> NettyGatewayServer.awaitBound(ch.newFailedFuture(new java.net.BindException("in use")),
        Duration.ofSeconds(1), "tcp", 9, "GATEWAY_LISTENERS_TCP_PORT"))
        .isInstanceOf(ListenerBindException.class).hasRootCauseMessage("in use");
  }
}
