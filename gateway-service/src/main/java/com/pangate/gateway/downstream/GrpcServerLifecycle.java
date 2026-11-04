package com.pangate.gateway.downstream;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.diagnostics.ListenerBindException;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.net.BindException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.springframework.context.SmartLifecycle;

/**
 * gRPC PushStream server on {@code gateway.grpc.port} (spec §3.7 phase MAX − 1700, §6.3): its own 2 event loops
 * ({@code gw-grpc-el}) and executor, separate from the data plane. mTLS with a SPIFFE check unless plaintext (local,
 * dev). Stop: no new streams, up to 5 s for open ones, then shutdownNow.
 */
public final class GrpcServerLifecycle implements SmartLifecycle {

  public static final int PHASE = Integer.MAX_VALUE - 1700;
  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(GrpcServerLifecycle.class);

  private final GatewayProperties.Grpc cfg;
  private final PushStreamService service;
  private final Executor executor;
  private volatile Server server;
  private volatile EventLoopGroup group;

  public GrpcServerLifecycle(GatewayProperties.Grpc cfg, PushStreamService service, Executor executor) {
    this.cfg = cfg;
    this.service = service;
    this.executor = executor;
  }

  @Override
  public void start() {
    ServerServiceDefinition def = cfg.plaintext()
        ? ServerInterceptors.intercept(service, new PushVersionInterceptor())
        : ServerInterceptors.intercept(service, new PushVersionInterceptor(),
            new SpiffeAuthInterceptor(cfg.allowedSpiffe()));
    EventLoopGroup g = new NioEventLoopGroup(2, new DefaultThreadFactory("gw-grpc-el", true));
    NettyServerBuilder b = NettyServerBuilder.forPort(cfg.port())
        .bossEventLoopGroup(g)
        .workerEventLoopGroup(g)
        .channelType(NioServerSocketChannel.class)
        .executor(executor)
        .addService(def);
    try {
      if (!cfg.plaintext()) {
        b.sslContext(GrpcSslContexts.forServer(cfg.tls().certFile().toFile(), cfg.tls().keyFile().toFile())
            .trustManager(cfg.tls().trustFile().toFile())
            .clientAuth(ClientAuth.REQUIRE)
            .build());
      }
      server = b.build().start();
      group = g;
      log.info("gRPC PushStream listening on {} ({})", server.getPort(), cfg.plaintext() ? "plaintext" : "mTLS");
    } catch (IOException e) {
      g.shutdownGracefully(0, 0, TimeUnit.SECONDS);
      if (e instanceof BindException || e.getCause() instanceof BindException) {
        throw new ListenerBindException("gRPC", cfg.port(), "GATEWAY_GRPC_PORT", e);
      }
      throw new IllegalStateException("gRPC server failed to start", e);
    } catch (RuntimeException e) {
      g.shutdownGracefully(0, 0, TimeUnit.SECONDS);
      throw e;
    }
  }

  @Override
  public void stop() {
    Server s = server;
    if (s == null) {
      return;
    }
    s.shutdown();
    try {
      if (!s.awaitTermination(5, TimeUnit.SECONDS)) {
        s.shutdownNow();
        s.awaitTermination(1, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      s.shutdownNow();
      Thread.currentThread().interrupt();
    } finally {
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      server = null;
    }
  }

  @Override
  public boolean isRunning() {
    return server != null;
  }

  @Override
  public int getPhase() {
    return PHASE;
  }

  /** The bound port (useful with port 0 in tests). */
  public int port() {
    return server.getPort();
  }
}
