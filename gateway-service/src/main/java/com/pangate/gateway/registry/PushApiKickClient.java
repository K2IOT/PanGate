package com.pangate.gateway.registry;

import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.Kick;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushServiceGrpc;
import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.metrics.GatewayMetrics;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * KICK through the Push API (spec §6.6): one long-lived {@code PushService.SendStream}, opened lazily and again after
 * an error. Each kick waits up to {@code timeout} for its {@code PushResponse}; two retries (100 ms, 400 ms), then it
 * is dropped and counted in {@code gw.kick.failures}.
 */
public final class PushApiKickClient implements KickClient, AutoCloseable {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(PushApiKickClient.class);
  private static final long[] RETRY_DELAYS_MS = {100, 400};

  private final ManagedChannel channel;
  private final ScheduledExecutorService scheduler;
  private final Duration timeout;
  private final Counter failures;

  public PushApiKickClient(ManagedChannel channel, ScheduledExecutorService scheduler, GatewayMetrics m,
      Duration timeout) {
    this.channel = channel;
    this.scheduler = scheduler;
    this.timeout = timeout;
    this.failures = m.counter("gw.kick.failures");
  }

  /** Channel to {@code gateway.push-api.target}: plaintext (dev), or mTLS with the gateway's gRPC certificate. */
  public static PushApiKickClient create(GatewayProperties.PushApi cfg, GatewayProperties.GrpcTls tls,
      ScheduledExecutorService scheduler, GatewayMetrics m) {
    NettyChannelBuilder b = NettyChannelBuilder.forTarget(cfg.target());
    if (cfg.plaintext()) {
      b.usePlaintext();
    } else {
      try {
        b.sslContext(GrpcSslContexts.forClient()
            .trustManager(SpiffeServerTrustManager.fromBundle(tls.trustFile()))
            .keyManager(tls.certFile().toFile(), tls.keyFile().toFile()).build());
      } catch (java.io.IOException | java.security.GeneralSecurityException e) {
        throw new IllegalStateException("push-api TLS: " + e.getMessage(), e);
      }
    }
    return new PushApiKickClient(b.build(), scheduler, m, Duration.ofSeconds(2));
  }

  @Override
  public void kick(String tenant, String principal, String newConnId, long connectedAtMs) {
    kickAsync(tenant, principal, newConnId, connectedAtMs);
  }

  /** Completes true once the Push API accepted the KICK, false after the last retry failed. Never exceptionally. */
  public CompletionStage<Boolean> kickAsync(String tenant, String principal, String newConnId, long connectedAtMs) {
    CompletableFuture<Boolean> result = new CompletableFuture<>();
    ControlOp op = ControlOp.newBuilder()
        .setKick(Kick.newBuilder().setNewConnId(newConnId).setConnectedAtMs(connectedAtMs)).build();
    // Off the caller's thread (an event loop): UUID generation, the stream lock and the first call setup
    // must never run on gw-io (spec §3.2).
    scheduler.execute(() -> attempt(tenant, principal, op, 0, result));
    return result;
  }

  private void attempt(String tenant, String principal, ControlOp op, int n, CompletableFuture<Boolean> result) {
    String id = UUID.randomUUID().toString();
    CompletableFuture<PushResponse> reply = new CompletableFuture<>();
    try {
      send(PushRequest.newBuilder().setRequestId(id).setKind(PushRequest.Kind.KIND_CONTROL).setTenantId(tenant)
          .setPrincipalId(principal).setControl(op).build(), reply);
    } catch (RuntimeException e) {
      reply.completeExceptionally(e);
    }
    reply.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS).whenComplete((r, e) -> {
      if (e == null && r.getOk()) {
        result.complete(true);
      } else if (n < RETRY_DELAYS_MS.length) {
        scheduler.schedule(() -> attempt(tenant, principal, op, n + 1, result), RETRY_DELAYS_MS[n],
            TimeUnit.MILLISECONDS);
      } else {
        failures.increment();
        log.warn("KICK via push-api failed after {} attempts: {}", n + 1,
            e != null ? e.toString() : r.getError());
        result.complete(false);
      }
    });
  }

  /** One SendStream and the requests sent on it: when it ends, only those fail. */
  private final class Call implements StreamObserver<PushResponse> {
    final Map<String, CompletableFuture<PushResponse>> sent = new ConcurrentHashMap<>();
    StreamObserver<PushRequest> requests;

    @Override
    public void onNext(PushResponse v) {
      CompletableFuture<PushResponse> f = sent.remove(v.getRequestId());
      if (f != null) {
        f.complete(v);
      }
    }

    @Override
    public void onError(Throwable t) {
      ended(this, t);
    }

    @Override
    public void onCompleted() {
      ended(this, new IllegalStateException("push-api closed the stream"));
    }
  }

  private Call call;  // guarded by this

  private synchronized void send(PushRequest r, CompletableFuture<PushResponse> reply) {
    if (call == null) {
      Call c = new Call();
      c.requests = PushServiceGrpc.newStub(channel).sendStream(c);
      call = c;
    }
    Call c = call;
    c.sent.put(r.getRequestId(), reply);
    reply.whenComplete((v, e) -> c.sent.remove(r.getRequestId()));  // timed out or answered: forget it
    c.requests.onNext(r);
  }

  /** The next kick opens a new stream; requests sent on the ended one fail now (and are retried). */
  private void ended(Call c, Throwable t) {
    synchronized (this) {
      if (call == c) {
        call = null;
      }
    }
    c.sent.values().forEach(f -> f.completeExceptionally(t));
    c.sent.clear();
  }

  @Override
  public void close() {
    channel.shutdownNow();
  }
}
