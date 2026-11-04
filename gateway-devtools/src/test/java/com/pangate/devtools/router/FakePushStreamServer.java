package com.pangate.devtools.router;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.DeliveryAck;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushBatch;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.PushStreamGrpc;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** A gateway stand-in: records batches and version headers, answers ACKED for every RELIABLE message. */
final class FakePushStreamServer implements AutoCloseable {

  static final Metadata.Key<String> VERSION = Metadata.Key.of("pangate-push-version", Metadata.ASCII_STRING_MARSHALLER);

  final List<PushBatch> batches = new CopyOnWriteArrayList<>();
  final List<String> versions = new CopyOnWriteArrayList<>();
  volatile boolean ack = true;
  private final Server server;

  FakePushStreamServer() throws Exception {
    PushStreamGrpc.PushStreamImplBase impl = new PushStreamGrpc.PushStreamImplBase() {
      @Override
      public StreamObserver<PushBatch> stream(StreamObserver<DeliveryAck> out) {
        return new StreamObserver<>() {
          @Override
          public void onNext(PushBatch b) {
            batches.add(b);
            DeliveryAck.Builder a = DeliveryAck.newBuilder();
            for (PushItem i : b.getItemsList()) {
              if (ack && i.getFrame().getClass_() == DeliveryClass.DELIVERY_CLASS_RELIABLE) {
                a.addAcks(AckItem.newBuilder().setDeliveryId(i.getDeliveryId()).setStatus(AckItem.Status.STATUS_ACKED));
              }
            }
            if (a.getAcksCount() > 0) {
              out.onNext(a.build());
            }
          }

          @Override public void onError(Throwable t) { }
          @Override public void onCompleted() { out.onCompleted(); }
        };
      }
    };
    server = NettyServerBuilder.forPort(0).addService(ServerInterceptors.intercept(impl,
        new io.grpc.ServerInterceptor() {
          @Override
          public <Q, R> io.grpc.ServerCall.Listener<Q> interceptCall(io.grpc.ServerCall<Q, R> call, Metadata h,
              io.grpc.ServerCallHandler<Q, R> next) {
            versions.add(String.valueOf(h.get(VERSION)));
            return Contexts.interceptCall(io.grpc.Context.current(), call, h, next);
          }
        })).build().start();
  }

  String target() {
    return "localhost:" + server.getPort();
  }

  @Override
  public void close() {
    server.shutdownNow();
  }
}
