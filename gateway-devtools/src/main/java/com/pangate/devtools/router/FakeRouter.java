package com.pangate.devtools.router;

import com.google.protobuf.ByteString;
import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.DeliveryAck;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.PushBatch;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.PushStreamGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stand-in for the Push Router (spec §2.3): one PushStream to a gateway (plaintext, {@code pangate-push-version: 1}).
 * Sends unicast, broadcast and control items and collects the gateway's acks. Usable as a library (tests, smoke).
 */
public final class FakeRouter implements AutoCloseable {

  public static final Metadata.Key<String> VERSION =
      Metadata.Key.of("pangate-push-version", Metadata.ASCII_STRING_MARSHALLER);

  private final ManagedChannel channel;
  private final StreamObserver<PushBatch> out;
  private final AtomicLong nextId = new AtomicLong();
  private final Map<Long, AckItem> byId = new ConcurrentHashMap<>();
  private final List<AckItem> acks = new CopyOnWriteArrayList<>();
  private volatile Throwable error;
  private volatile boolean ended;

  private FakeRouter(ManagedChannel channel) {
    this.channel = channel;
    Metadata h = new Metadata();
    h.put(VERSION, "1");
    out = PushStreamGrpc.newStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(h))
        .stream(new StreamObserver<>() {
          @Override
          public void onNext(DeliveryAck v) {
            for (AckItem a : v.getAcksList()) {
              acks.add(a);
              byId.put(a.getDeliveryId(), a);
            }
          }

          @Override
          public void onError(Throwable t) {
            error = t;
            ended = true;
          }

          @Override
          public void onCompleted() {
            ended = true;
          }
        });
  }

  /** {@code target}: host:port or a gRPC target URI. */
  public static FakeRouter connect(String target) {
    return new FakeRouter(NettyChannelBuilder.forTarget(target).usePlaintext().build());
  }

  /** RELIABLE / ORDERED / EPHEMERAL PUSH to one connection; returns its delivery id. */
  public long unicast(String connId, DeliveryClass cls, String msgId, byte[] payload) {
    return unicast(connId, Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setMsgId(msgId).setClass_(cls)
        .setPayload(ByteString.copyFrom(payload)).build());
  }

  /** Any pre-built frame (PUSH, RPC_REQ) to one connection; returns its delivery id. */
  public long unicast(String connId, Frame frame) {
    long id = nextId.incrementAndGet();
    send(PushBatch.newBuilder().addItems(PushItem.newBuilder().setDeliveryId(id).setKind(PushItem.Kind.KIND_MESSAGE)
        .setConnId(connId).setFrame(frame).setEnqueuedAtMs(System.currentTimeMillis())).build());
    return id;
  }

  public void broadcast(String tenant, String topic, PushItem.Priority priority, byte[] payload) {
    send(PushBatch.newBuilder().addItems(PushItem.newBuilder().setDeliveryId(nextId.incrementAndGet())
        .setKind(PushItem.Kind.KIND_BROADCAST).setTenantId(tenant).setTopic(topic).setPriority(priority)
        .setFrame(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setTopic(topic)
            .setMsgId("b-" + nextId.get()).setClass_(DeliveryClass.DELIVERY_CLASS_EPHEMERAL)
            .setPayload(ByteString.copyFrom(payload)))
        .setEnqueuedAtMs(System.currentTimeMillis())).build());
  }

  public void control(String tenant, String principal, ControlOp op) {
    send(PushBatch.newBuilder().addItems(PushItem.newBuilder().setDeliveryId(nextId.incrementAndGet())
        .setKind(PushItem.Kind.KIND_CONTROL).setTenantId(tenant).setPrincipalId(principal).setControl(op)).build());
  }

  public synchronized void send(PushBatch batch) {
    out.onNext(batch);
  }

  public AckItem awaitAck(long deliveryId, Duration timeout) throws TimeoutException, InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      AckItem a = byId.get(deliveryId);
      if (a != null) {
        return a;
      }
      if (ended) {
        throw new IllegalStateException("PushStream ended: " + error);
      }
      TimeUnit.MILLISECONDS.sleep(5);
    }
    throw new TimeoutException("no ack for delivery " + deliveryId + " within " + timeout);
  }

  public List<AckItem> acks() {
    return new ArrayList<>(acks);
  }

  /** The stream failed or was closed by the gateway. */
  public boolean ended() {
    return ended;
  }

  @Override
  public void close() {
    try {
      synchronized (this) {
        if (!ended) {
          out.onCompleted();
        }
      }
    } catch (RuntimeException ignored) {
      // already gone
    }
    channel.shutdownNow();
  }
}
