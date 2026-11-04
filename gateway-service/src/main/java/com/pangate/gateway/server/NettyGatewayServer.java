package com.pangate.gateway.server;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.diagnostics.ListenerBindException;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.EventLoopLagProbe;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.shed.AdmissionController;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Data-plane lifecycle (spec §3.7): starts last (after the management port and dependencies), stops first. Binds
 * the WSS listener (N acceptors with SO_REUSEPORT on epoll) and the TCP listener.
 */
public final class NettyGatewayServer implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(NettyGatewayServer.class);
  public static final int PHASE = Integer.MAX_VALUE - 100;

  private final GatewayProperties props;
  private final IoGroups io;
  private final LoopRegistry loops;
  private final NodeState node;
  private final AdmissionController admission;
  private final EventLoopLagProbe lagProbe;
  private final ChannelInitializer<SocketChannel> wsInitializer;
  private final ChannelInitializer<SocketChannel> tcpInitializer;
  private final com.pangate.gateway.lifecycle.DrainCoordinator drain;
  private final List<Channel> listeners = new ArrayList<>();
  private volatile boolean running;
  private final java.util.concurrent.atomic.AtomicBoolean stopping = new java.util.concurrent.atomic.AtomicBoolean();

  public NettyGatewayServer(GatewayProperties props, IoGroups io, LoopRegistry loops, NodeState node,
      AdmissionController admission, EventLoopLagProbe lagProbe, ChannelInitializer<SocketChannel> wsInitializer,
      ChannelInitializer<SocketChannel> tcpInitializer, com.pangate.gateway.lifecycle.DrainCoordinator drain) {
    this.drain = drain;
    this.props = props;
    this.io = io;
    this.loops = loops;
    this.node = node;
    this.admission = admission;
    this.lagProbe = lagProbe;
    this.wsInitializer = wsInitializer;
    this.tcpInitializer = tcpInitializer;
  }

  @Override
  public void start() {
    loops.all().forEach(LoopContext::startTicker);
    lagProbe.start();
    int ws = props.listeners().ws().port();
    try {
      for (int i = 0; i < io.effectiveAcceptors(props.listeners().ws().acceptors()); i++) {
        addListener(bind(wsInitializer, ws, "ws", "GATEWAY_LISTENERS_WS_PORT"));
      }
      if (props.listeners().tcp().enabled()) {
        addListener(bind(tcpInitializer, props.listeners().tcp().port(), "tcp", "GATEWAY_LISTENERS_TCP_PORT"));
      }
    } catch (RuntimeException e) {
      // Fail fast (spec §3.7): Spring will not call stop() on a lifecycle that never started, so release here —
      // otherwise the already-bound listeners keep accepting and the non-daemon gw-io threads keep the JVM alive.
      listeners.forEach(ch -> ch.close().syncUninterruptibly());
      listeners.clear();
      io.shutdown(Duration.ZERO, Duration.ofSeconds(5)).syncUninterruptibly();
      throw e;
    }
    node.listenersBound(true);
    admission.markReady();
    running = true;
    log.info("gateway listening: wss :{}{} ({} acceptors), tcp :{}", ws, props.listeners().ws().path(),
        listeners.size() - (props.listeners().tcp().enabled() ? 1 : 0), props.listeners().tcp().port());
  }

  private synchronized void addListener(Channel ch) {
    listeners.add(ch);
  }

  private Channel bind(ChannelInitializer<SocketChannel> init, int port, String name, String envVar) {
    ServerBootstrap b = new ServerBootstrap().group(io.group()).channel(io.serverChannelClass())
        .option(ChannelOption.SO_BACKLOG, 65_535)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_KEEPALIVE, false)
        .childOption(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(64, 1024, 65_536))
        .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(32 * 1024, 64 * 1024))
        .childHandler(init);
    if (io.transport() == Transport.EPOLL) {
      b.option(EpollChannelOption.SO_REUSEPORT, true);
    }
    try {
      return b.bind(new InetSocketAddress(port)).syncUninterruptibly().channel();
    } catch (Exception e) {
      throw new ListenerBindException(name, port, envVar, e);
    }
  }

  /** Starts the staged drain (spec §7.5) without stopping the process; idempotent. */
  public CompletableFuture<Void> drain() {
    return drain.drain(this::closeListeners);
  }

  private synchronized void closeListeners() {
    listeners.forEach(Channel::close);
    listeners.clear();
  }

  @Override
  public void stop(Runnable callback) {
    // /admin/drain may already have started the drain: stop waits for the same one, then shuts the loops down.
    if (!stopping.compareAndSet(false, true)) {
      callback.run();
      return;
    }
    drain().whenComplete((v, e) -> io.shutdown(Duration.ZERO, Duration.ofSeconds(5)).addListener(f -> {
      running = false;
      callback.run();
    }));
  }

  @Override
  public void stop() {
    CompletableFuture<Void> done = new CompletableFuture<>();
    stop(() -> done.complete(null));
    done.join();
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public int getPhase() {
    return PHASE;
  }
}
