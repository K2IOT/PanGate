package com.pangate.gateway.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.pangate.gateway.config.GatewayProperties;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class IoGroupsTest {

  @Test
  void autoPicksEpollOnLinux() {
    assumeTrue(Epoll.isAvailable());
    IoGroups io = new IoGroups(new GatewayProperties.Io(2, Transport.AUTO));
    try {
      assertThat(io.transport()).isEqualTo(Transport.EPOLL);
      assertThat(io.serverChannelClass()).isEqualTo(EpollServerSocketChannel.class);
      assertThat(io.effectiveAcceptors(4)).isEqualTo(4);
    } finally {
      io.shutdown(Duration.ZERO, Duration.ofSeconds(2)).syncUninterruptibly();
    }
  }

  @Test
  void nioClampsAcceptorsToOneAndNamesThreads() throws Exception {
    IoGroups io = new IoGroups(new GatewayProperties.Io(2, Transport.NIO));
    try {
      assertThat(io.transport()).isEqualTo(Transport.NIO);
      assertThat(io.serverChannelClass()).isEqualTo(NioServerSocketChannel.class);
      assertThat(io.effectiveAcceptors(4)).isEqualTo(1);
      AtomicReference<String> name = new AtomicReference<>();
      io.group().next().submit(() -> name.set(Thread.currentThread().getName())).sync();
      assertThat(name.get()).startsWith("gw-io-");
    } finally {
      io.shutdown(Duration.ZERO, Duration.ofSeconds(2)).syncUninterruptibly();
    }
  }

  @Test
  void zeroThreadsMeansAvailableProcessors() {
    IoGroups io = new IoGroups(new GatewayProperties.Io(0, Transport.NIO));
    try {
      assertThat(io.threads()).isEqualTo(Runtime.getRuntime().availableProcessors());
    } finally {
      io.shutdown(Duration.ZERO, Duration.ofSeconds(2)).syncUninterruptibly();
    }
  }
}
