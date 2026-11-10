package com.pangate.pushapi.grpc;

import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushServiceGrpc;
import com.pangate.pushapi.core.Caller;
import com.pangate.pushapi.core.KeyedSerialExecutor;
import com.pangate.pushapi.core.PushAcceptor;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PushService.SendStream} (push-api spec §2.3): each request runs on a virtual thread, requests of one target
 * in arrival order; at most {@code maxInFlight} unanswered requests per stream (inbound flow control); responses are
 * matched by {@code request_id}.
 */
public final class PushGrpcService extends PushServiceGrpc.PushServiceImplBase {

  private static final Logger log = LoggerFactory.getLogger(PushGrpcService.class);

  private final PushAcceptor acceptor;
  private final KeyedSerialExecutor serial;
  private final int maxInFlight;
  private final MeterRegistry meters;

  public PushGrpcService(PushAcceptor acceptor, KeyedSerialExecutor serial, int maxInFlight, MeterRegistry meters) {
    this.acceptor = acceptor;
    this.serial = serial;
    this.maxInFlight = maxInFlight;
    this.meters = meters;
  }

  static String targetKey(PushRequest r) {
    return r.getKind() == PushRequest.Kind.KIND_BROADCAST ? "topic:" + r.getTopic() : r.getPrincipalId();
  }

  @Override
  public StreamObserver<PushRequest> sendStream(StreamObserver<PushResponse> out) {
    ServerCallStreamObserver<PushResponse> so = (ServerCallStreamObserver<PushResponse>) out;
    so.disableAutoRequest();
    so.request(maxInFlight);
    Caller caller = new Caller.InternalCaller(SpiffeInterceptor.SPIFFE_ID.get());
    AtomicInteger inFlight = new AtomicInteger();
    AtomicBoolean halfClosed = new AtomicBoolean();
    AtomicBoolean done = new AtomicBoolean();
    Runnable finishIfIdle = () -> {
      if (halfClosed.get() && inFlight.get() == 0 && done.compareAndSet(false, true)) {
        synchronized (so) {
          so.onCompleted();
        }
      }
    };
    so.setOnCancelHandler(() -> done.set(true));
    return new StreamObserver<>() {
      @Override
      public void onNext(PushRequest r) {
        inFlight.incrementAndGet();
        serial.submit(targetKey(r) + "|" + System.identityHashCode(so), () -> acceptor.accept(caller, r))
            .whenComplete((res, err) -> {
              PushResponse answer = res != null ? res : PushResponse.newBuilder().setRequestId(r.getRequestId())
                  .setOk(false).setCode(PushResponse.Code.CODE_UNAVAILABLE).setError("internal").build();
              if (err != null) {
                log.error("push {} failed: {}", r.getRequestId(), err.toString());
              }
              meters.counter("pushapi.requests", "surface", "grpc", "code", answer.getCode().name()).increment();
              if (!done.get()) {
                synchronized (so) {
                  so.onNext(answer);
                }
                so.request(1);
              }
              inFlight.decrementAndGet();
              finishIfIdle.run();
            });
      }

      @Override
      public void onError(Throwable t) {
        done.set(true);
      }

      @Override
      public void onCompleted() {
        halfClosed.set(true);
        finishIfIdle.run();
      }
    };
  }
}
