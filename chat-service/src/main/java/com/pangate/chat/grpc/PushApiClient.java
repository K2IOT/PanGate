package com.pangate.chat.grpc;

import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.PushUnavailableException;
import com.pangate.chat.core.Pusher;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * One long-lived {@code PushService.SendStream} (as the gateway's {@code PushApiKickClient}): opened lazily and again
 * after it ends; answers matched by {@code request_id}; each request waits at most {@code timeout}. A request whose
 * stream ended or that timed out fails with {@link PushUnavailableException}.
 */
public final class PushApiClient implements Pusher, AutoCloseable {

  private final ManagedChannel channel;
  private final Duration timeout;
  private Call call;   // guarded by this

  public PushApiClient(ManagedChannel channel, Duration timeout) {
    this.channel = channel;
    this.timeout = timeout;
  }

  /**
   * Plaintext (local, dev) or TLS. mTLS with the chat-service SPIFFE certificate (prod, chat spec §6) is not wired in
   * CH1: TLS here verifies the server against the JVM trust store only.
   */
  public static PushApiClient create(ChatProperties.PushApi cfg) {
    NettyChannelBuilder b = NettyChannelBuilder.forTarget(cfg.target());
    if (cfg.plaintext()) {
      b.usePlaintext();
    }
    return new PushApiClient(b.build(), cfg.timeout());
  }

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

  @Override
  public CompletableFuture<PushResponse> send(PushRequest r) {
    CompletableFuture<PushResponse> reply = new CompletableFuture<>();
    try {
      synchronized (this) {
        if (call == null) {
          Call c = new Call();
          c.requests = PushServiceGrpc.newStub(channel).sendStream(c);
          call = c;
        }
        Call c = call;
        c.sent.put(r.getRequestId(), reply);
        reply.whenComplete((v, e) -> c.sent.remove(r.getRequestId()));
        c.requests.onNext(r);   // StreamObserver is not thread-safe: sends stay under the lock
      }
    } catch (RuntimeException e) {
      reply.completeExceptionally(e);
    }
    return reply.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS).handle((v, e) -> {
      if (e == null) {
        return CompletableFuture.completedFuture(v);
      }
      Throwable cause = e instanceof java.util.concurrent.CompletionException && e.getCause() != null
          ? e.getCause() : e;
      return CompletableFuture.<PushResponse>failedFuture(new PushUnavailableException(
          cause instanceof TimeoutException ? "push-api did not answer in " + timeout : cause.toString(), cause));
    }).thenCompose(f -> f);
  }

  /** The next send opens a new stream; requests on the ended one fail now. */
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
