package com.pangate.chat.grpc;

import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** A Push API that answers only when told to: requests on the wire, outstanding answers, streams it can end. */
public final class ScriptedPushService extends PushServiceGrpc.PushServiceImplBase {

  /** Every request received, in arrival order, with the stream it came on. */
  public record Received(int stream, PushRequest request) {}

  public final List<Received> received = new CopyOnWriteArrayList<>();
  public final AtomicInteger streams = new AtomicInteger();
  public volatile boolean autoAnswer;
  /** Ask for one message at a time (the transport reports not ready in between), instead of the whole window. */
  public volatile boolean oneAtATime;
  private final Map<String, StreamObserver<PushResponse>> pending = new ConcurrentHashMap<>();
  private final List<StreamObserver<PushResponse>> open = new CopyOnWriteArrayList<>();

  @Override
  public StreamObserver<PushRequest> sendStream(StreamObserver<PushResponse> out) {
    // as PushGrpcService: manual flow control, the whole window requested up front
    io.grpc.stub.ServerCallStreamObserver<PushResponse> so = (io.grpc.stub.ServerCallStreamObserver<PushResponse>) out;
    so.disableAutoRequest();
    so.request(oneAtATime ? 1 : 1_000_000);
    int id = streams.incrementAndGet();
    open.add(out);
    return new StreamObserver<>() {
      @Override
      public void onNext(PushRequest r) {
        received.add(new Received(id, r));
        if (oneAtATime) {
          so.request(1);
        }
        pending.put(r.getRequestId(), out);
        if (autoAnswer) {
          answer(r.getRequestId());
        }
      }

      @Override
      public void onError(Throwable t) {
        open.remove(out);
      }

      @Override
      public void onCompleted() {
        open.remove(out);
        synchronized (out) {
          out.onCompleted();
        }
      }
    };
  }

  /** Answers one request (by the id on the wire) with OK. */
  public void answer(String wireId) {
    StreamObserver<PushResponse> out = pending.remove(wireId);
    if (out != null) {
      synchronized (out) {
        out.onNext(PushResponse.newBuilder().setRequestId(wireId).setOk(true).setCode(PushResponse.Code.CODE_OK)
            .setMsgId("m-" + wireId).build());
      }
    }
  }

  /** Answers the request received n-th (0-based). */
  public void answerReceived(int n) {
    answer(received.get(n).request().getRequestId());
  }

  public int outstanding() {
    return pending.size();
  }

  public List<PushRequest> requests() {
    List<PushRequest> out = new ArrayList<>();
    received.forEach(r -> out.add(r.request()));
    return out;
  }

  /** Ends every open stream with an error, as a Push API that went away. */
  public void endAll() {
    for (StreamObserver<PushResponse> out : open) {
      synchronized (out) {
        out.onError(Status.UNAVAILABLE.withDescription("scripted end").asRuntimeException());
      }
    }
    open.clear();
    pending.clear();
  }
}
