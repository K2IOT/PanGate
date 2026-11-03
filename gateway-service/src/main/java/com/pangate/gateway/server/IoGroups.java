package com.pangate.gateway.server;

import com.pangate.gateway.config.GatewayProperties;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The data-plane event loop group ("gw-io-*"): epoll when available, NIO otherwise (spec §3.2). */
public final class IoGroups {

  private static final Logger log = LoggerFactory.getLogger(IoGroups.class);

  private final Transport transport;
  private final int threads;
  private final EventLoopGroup group;

  public IoGroups(GatewayProperties.Io io) {
    this.transport = resolve(io.transport());
    this.threads = io.threads() > 0 ? io.threads() : Runtime.getRuntime().availableProcessors();
    IoHandlerFactory factory = transport == Transport.EPOLL ? EpollIoHandler.newFactory() : NioIoHandler.newFactory();
    this.group = new MultiThreadIoEventLoopGroup(threads, new DefaultThreadFactory("gw-io"), factory);
    log.info("data plane: {} transport, {} event loops", transport, threads);
  }

  private static Transport resolve(Transport configured) {
    if (configured == Transport.AUTO) {
      return Epoll.isAvailable() ? Transport.EPOLL : Transport.NIO;
    }
    if (configured == Transport.EPOLL) {
      Epoll.ensureAvailability();
    }
    return configured;
  }

  public Transport transport() {
    return transport;
  }

  public int threads() {
    return threads;
  }

  public EventLoopGroup group() {
    return group;
  }

  public Class<? extends ServerChannel> serverChannelClass() {
    return transport == Transport.EPOLL ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
  }

  /** SO_REUSEPORT exists only on epoll; binding the same port twice fails elsewhere. */
  public int effectiveAcceptors(int configured) {
    if (transport != Transport.EPOLL && configured > 1) {
      log.info("{} transport has no SO_REUSEPORT: using 1 acceptor instead of {}", transport, configured);
      return 1;
    }
    return Math.max(1, configured);
  }

  public Future<?> shutdown(Duration quiet, Duration timeout) {
    return group.shutdownGracefully(quiet.toMillis(), timeout.toMillis(), TimeUnit.MILLISECONDS);
  }
}
