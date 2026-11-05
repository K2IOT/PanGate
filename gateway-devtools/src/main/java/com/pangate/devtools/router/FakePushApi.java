package com.pangate.devtools.router;

import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushServiceGrpc;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Stand-in for the Push API (spec §2.3, §6.6): receives the gateways' KICK on {@code PushService.SendStream} and
 * forwards it as {@code PushItem CONTROL} to every configured gateway through a {@link FakeRouter}. Serves gRPC health.
 */
public final class FakePushApi implements AutoCloseable {

  private final Server server;
  private final List<String> routerTargets;
  private final Map<String, FakeRouter> routers = new ConcurrentHashMap<>();
  private final List<PushRequest> received = new CopyOnWriteArrayList<>();

  private FakePushApi(int port, List<String> routerTargets) {
    this.routerTargets = List.copyOf(routerTargets);
    HealthStatusManager health = new HealthStatusManager();
    try {
      server = NettyServerBuilder.forPort(port).addService(new Service()).addService(health.getHealthService())
          .build().start();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    health.setStatus("", io.grpc.health.v1.HealthCheckResponse.ServingStatus.SERVING);
  }

  public static FakePushApi start(int port, List<String> routerTargets) {
    return new FakePushApi(port, routerTargets);
  }

  public int port() {
    return server.getPort();
  }

  public List<PushRequest> received() {
    return new ArrayList<>(received);
  }

  /** Lazily connected (a gateway may start after this process); replaced once its stream ended. */
  private FakeRouter router(String target) {
    return routers.compute(target, (t, r) -> r == null || r.ended() ? FakeRouter.connect(t) : r);
  }

  private final class Service extends PushServiceGrpc.PushServiceImplBase {
    @Override
    public StreamObserver<PushRequest> sendStream(StreamObserver<PushResponse> out) {
      return new StreamObserver<>() {
        @Override
        public void onNext(PushRequest r) {
          received.add(r);
          PushResponse.Builder res = PushResponse.newBuilder().setRequestId(r.getRequestId());
          try {
            if (r.getKind() != PushRequest.Kind.KIND_CONTROL) {
              throw new IllegalArgumentException("unsupported kind " + r.getKind());
            }
            for (String t : routerTargets) {
              router(t).control(r.getTenantId(), r.getPrincipalId(), r.getControl());
            }
            res.setOk(true);
          } catch (RuntimeException e) {
            res.setOk(false).setError(e.toString());
          }
          synchronized (out) {
            out.onNext(res.build());
          }
        }

        @Override
        public void onError(Throwable t) { }

        @Override
        public void onCompleted() {
          synchronized (out) {
            out.onCompleted();
          }
        }
      };
    }
  }

  @Override
  public void close() {
    routers.values().forEach(FakeRouter::close);
    server.shutdownNow();
  }
}
