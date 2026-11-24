package com.pangate.chat.grpc;

import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.Pusher;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Perf 3b (spec §4.2): {@code streams} long-lived {@code PushService.SendStream} calls on one channel whose
 * {@code round_robin} policy spreads them over every Push API address DNS returns (a headless service in Kubernetes).
 * A push goes on the stream of its target, as the server orders per target ({@code PushGrpcService.targetKey}), so
 * one recipient's pushes keep their order. Each stream has its own window and priority queues ({@link PushStream}).
 * mTLS with the chat-service SPIFFE certificate (prod, chat spec §6) is not wired yet (README: TODO before production).
 */
public final class PushApiClient implements Pusher, AutoCloseable {

  private final ManagedChannel channel;
  private final ScheduledExecutorService timers;
  private final PushStream[] streams;

  public PushApiClient(ManagedChannel channel, ChatProperties.PushApi cfg, MeterRegistry meters) {
    this.channel = channel;
    this.timers = Executors.newSingleThreadScheduledExecutor(r -> Thread.ofPlatform().daemon().name("push-timers")
        .unstarted(r));
    PushStream.Limits limits = new PushStream.Limits(cfg.window(), cfg.lowShare(), cfg.lowWait(), cfg.queueMax(),
        cfg.mustWait(), cfg.timeout());
    this.streams = new PushStream[Math.max(1, cfg.streams())];
    for (int i = 0; i < streams.length; i++) {
      streams[i] = new PushStream(obs -> PushServiceGrpc.newStub(channel).sendStream(obs), limits, System::nanoTime,
          timers, meters, i);
    }
  }

  /** Tests: the defaults with this {@code timeout}. */
  public PushApiClient(ManagedChannel channel, Duration timeout) {
    this(channel, new ChatProperties.PushApi("", true, timeout, 4, 1_000, 0.2, Duration.ofSeconds(2), 10_000,
        Duration.ofSeconds(10)), new SimpleMeterRegistry());
  }

  /** Plaintext (local, dev) or TLS verifying the server against the JVM trust store only. */
  public static PushApiClient create(ChatProperties.PushApi cfg, MeterRegistry meters) {
    NettyChannelBuilder b = NettyChannelBuilder.forTarget(cfg.target()).defaultLoadBalancingPolicy("round_robin");
    if (cfg.plaintext()) {
      b.usePlaintext();
    }
    return new PushApiClient(b.build(), cfg, meters);
  }

  /** The Push API's ordering key: the topic of a BROADCAST, else the principal. */
  static String streamKey(PushRequest r) {
    return r.getKind() == PushRequest.Kind.KIND_BROADCAST ? "topic:" + r.getTopic() : r.getPrincipalId();
  }

  @Override
  public CompletableFuture<PushResponse> send(PushRequest r, Pusher.Priority priority) {
    return streams[Math.floorMod(streamKey(r).hashCode(), streams.length)].send(r, priority);
  }

  @Override
  public void close() {
    for (PushStream s : streams) {
      s.close();
    }
    timers.shutdownNow();
    channel.shutdownNow();
  }
}
