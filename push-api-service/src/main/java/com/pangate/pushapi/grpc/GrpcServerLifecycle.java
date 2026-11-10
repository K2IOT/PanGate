package com.pangate.pushapi.grpc;

import com.pangate.pushapi.config.PushApiProperties;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/** The gRPC server on {@code push-api.grpc.port}; on stop, a graceful shutdown of at most 10 s (spec §5). */
public final class GrpcServerLifecycle implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(GrpcServerLifecycle.class);

  private final PushApiProperties.Grpc cfg;
  private final PushGrpcService service;
  private final ExecutorService executor;
  private volatile Server server;

  public GrpcServerLifecycle(PushApiProperties.Grpc cfg, PushGrpcService service, ExecutorService executor) {
    this.cfg = cfg;
    this.service = service;
    this.executor = executor;
  }

  @Override
  public void start() {
    NettyServerBuilder b = NettyServerBuilder.forPort(cfg.port()).executor(executor)
        .permitKeepAliveTime(10, TimeUnit.SECONDS).permitKeepAliveWithoutCalls(true)
        .addService(ServerInterceptors.intercept(service, new SpiffeInterceptor(cfg.allowedSpiffe(), cfg.plaintext())));
    try {
      if (!cfg.plaintext()) {
        b.sslContext(GrpcSslContexts.forServer(cfg.tls().cert().toFile(), cfg.tls().key().toFile())
            .trustManager(cfg.tls().trust().toFile()).clientAuth(ClientAuth.REQUIRE).build());
      }
      server = b.build().start();
      log.info("gRPC PushService listening on {} ({})", server.getPort(), cfg.plaintext() ? "plaintext" : "mTLS");
    } catch (IOException e) {
      throw new IllegalStateException("gRPC server failed to start on " + cfg.port(), e);
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
      if (!s.awaitTermination(10, TimeUnit.SECONDS)) {
        s.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      s.shutdownNow();
    }
    server = null;
  }

  @Override
  public boolean isRunning() {
    return server != null;
  }

  /** Starts after the background parts and stops before them (with the web server). */
  @Override
  public int getPhase() {
    return SmartLifecycle.DEFAULT_PHASE - 2048;
  }
}
