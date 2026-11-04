package com.pangate.gateway.downstream;

import com.pangate.contracts.v1.DeliveryAck;
import com.pangate.contracts.v1.PushBatch;
import com.pangate.contracts.v1.PushStreamGrpc;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;

/**
 * Router streams (spec §6.3). Manual flow control: request(8) up front, then one more per dispatched batch while
 * {@code shedLevel < 4}; otherwise the stream is paused until {@link #resumePaused()} (gw-tick, every 100 ms).
 */
public final class PushStreamService extends PushStreamGrpc.PushStreamImplBase {

  static final int INITIAL_REQUEST = 8;

  /** One open router stream. */
  private final class Stream {
    final ServerCallStreamObserver<DeliveryAck> out;
    final AckBatcher acks;
    final StreamRef ref;

    Stream(ServerCallStreamObserver<DeliveryAck> out) {
      this.out = out;
      this.acks = new AckBatcher(out, exec, metrics);
      this.ref = new StreamRef(acks);
    }

    void close(boolean complete) {
      ref.markDead();
      active.remove(this);
      paused.remove(this);
      acks.close(complete);
    }
  }

  private final BiConsumer<PushBatch, StreamRef> dispatcher;
  private final NodeState node;
  private final ScheduledExecutorService exec;
  private final GatewayMetrics metrics;
  private final Set<Stream> active = ConcurrentHashMap.newKeySet();
  private final Set<Stream> paused = ConcurrentHashMap.newKeySet();

  public PushStreamService(BiConsumer<PushBatch, StreamRef> dispatcher, NodeState node,
      ScheduledExecutorService exec, GatewayMetrics metrics) {
    this.dispatcher = dispatcher;
    this.node = node;
    this.exec = exec;
    this.metrics = metrics;
    metrics.gauge("gw.pushstream.active", active, Set::size);
  }

  @Override
  public StreamObserver<PushBatch> stream(StreamObserver<DeliveryAck> responseObserver) {
    ServerCallStreamObserver<DeliveryAck> out = (ServerCallStreamObserver<DeliveryAck>) responseObserver;
    out.disableAutoRequest();
    Stream st = new Stream(out);
    active.add(st);
    out.setOnCancelHandler(() -> st.close(false));
    st.acks.start();
    out.request(INITIAL_REQUEST);
    return new StreamObserver<>() {
      @Override
      public void onNext(PushBatch batch) {
        dispatcher.accept(batch, st.ref);
        if (node.shedLevel() < PushDispatcher.SHED_NACK_LEVEL) {
          out.request(1);
        } else {
          paused.add(st);
        }
      }

      @Override
      public void onError(Throwable t) {
        st.close(false);
      }

      @Override
      public void onCompleted() {
        st.close(true);
      }
    };
  }

  /** gw-tick: streams paused while shedding ask for the next batch once the level is below 4 again. */
  public void resumePaused() {
    if (node.shedLevel() >= PushDispatcher.SHED_NACK_LEVEL) {
      return;
    }
    for (Stream st : paused) {
      if (paused.remove(st)) {
        st.out.request(1);
      }
    }
  }

  public int activeStreams() {
    return active.size();
  }
}
