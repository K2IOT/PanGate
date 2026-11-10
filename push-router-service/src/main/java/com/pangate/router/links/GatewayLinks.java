package com.pangate.router.links;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.DeliveryAck;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushBatch;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.PushStreamGrpc;
import com.pangate.router.delivery.Links;
import com.pangate.router.nodes.Node;
import com.pangate.router.nodes.NodeDirectory;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One gRPC PushStream per gateway node (push-router spec §3.3): batches of up to {@code batchMax} items or after
 * {@code linger}, ACKs matched by delivery_id, at most {@code maxInFlight} items awaiting an ACK per node (send blocks
 * beyond it), every pending future completed with NACK_CLOSED when the stream ends or the node leaves / changes epoch.
 * The channel reconnects with gRPC's own backoff; a new stream opens on the next send.
 */
public final class GatewayLinks implements Links, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(GatewayLinks.class);
  static final Metadata.Key<String> VERSION = Metadata.Key.of("pangate-push-version", Metadata.ASCII_STRING_MARSHALLER);

  private final NodeDirectory dir;
  private final Function<Node, ManagedChannel> channels;
  private final int batchMax;
  private final long lingerNanos;
  private final int maxInFlight;
  private final AtomicLong deliveryIds = new AtomicLong();
  private final Map<String, Link> links = new ConcurrentHashMap<>();
  private final Map<String, ManagedChannel> channelByAddress = new ConcurrentHashMap<>();
  private final ScheduledExecutorService flusher = Executors.newSingleThreadScheduledExecutor(
      Thread.ofPlatform().daemon().name("router-flush").factory());

  public GatewayLinks(NodeDirectory dir, Function<Node, ManagedChannel> channels, int batchMax, Duration linger,
      int maxInFlight) {
    this.dir = dir;
    this.channels = channels;
    this.batchMax = batchMax;
    this.lingerNanos = linger.toNanos();
    this.maxInFlight = maxInFlight;
    dir.onChange((old, now) -> old.values().forEach(o -> {
      Node n = now.get(o.id());
      if (n == null || n.epoch() != o.epoch()) {
        closeNode(o.id());
      }
    }));
  }

  static boolean needsAck(PushItem item) {
    return item.getKind() == PushItem.Kind.KIND_MESSAGE
        && (item.getFrame().getClass_() == DeliveryClass.DELIVERY_CLASS_RELIABLE
        || item.getFrame().getClass_() == DeliveryClass.DELIVERY_CLASS_ORDERED);
  }

  @Override
  public CompletableFuture<AckItem.Status> send(String nodeId, PushItem item) {
    Node node = dir.node(nodeId).orElseThrow(() -> new NodeUnavailableException(nodeId));
    Link link = links.compute(nodeId, (id, l) -> l == null || l.closed.get() || l.node.epoch() != node.epoch()
        ? replace(l, node) : l);
    return link.send(item.toBuilder().setDeliveryId(deliveryIds.incrementAndGet()).build());
  }

  private Link replace(Link old, Node node) {
    if (old != null) {
      old.close();
    }
    ManagedChannel ch = channelByAddress.computeIfAbsent(node.address(), a -> channels.apply(node));
    return new Link(node, ch);
  }

  /** Ends the node's stream: every pending future → NACK_CLOSED. */
  public void closeNode(String nodeId) {
    Link l = links.remove(nodeId);
    if (l != null) {
      l.close();
    }
  }

  public int openLinks() {
    return (int) links.values().stream().filter(l -> !l.closed.get()).count();
  }

  @Override
  public void close() {
    links.keySet().forEach(this::closeNode);
    channelByAddress.values().forEach(ManagedChannel::shutdownNow);
    flusher.shutdownNow();
  }

  private record Pending(PushItem item, CompletableFuture<AckItem.Status> future, boolean awaitsAck) {}

  private final class Link implements ClientResponseObserver<PushBatch, DeliveryAck> {
    final Node node;
    final AtomicBoolean closed = new AtomicBoolean();
    final ConcurrentLinkedQueue<Pending> queue = new ConcurrentLinkedQueue<>();
    final Map<Long, Pending> awaiting = new ConcurrentHashMap<>();
    final Semaphore permits = new Semaphore(maxInFlight);
    final AtomicBoolean flushScheduled = new AtomicBoolean();
    volatile ClientCallStreamObserver<PushBatch> out;

    Link(Node node, ManagedChannel channel) {
      this.node = node;
      Metadata h = new Metadata();
      h.put(VERSION, "1");
      PushStreamGrpc.newStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(h)).stream(this);
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<PushBatch> requestStream) {
      out = requestStream;
    }

    CompletableFuture<AckItem.Status> send(PushItem item) {
      boolean ack = needsAck(item);
      if (ack) {
        try {
          permits.acquire();  // backpressure: blocks the (virtual) worker thread
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return CompletableFuture.completedFuture(AckItem.Status.STATUS_NACK_CLOSED);
        }
      }
      CompletableFuture<AckItem.Status> f = new CompletableFuture<>();
      Pending p = new Pending(item, f, ack);
      if (closed.get()) {
        finish(p, AckItem.Status.STATUS_NACK_CLOSED);
        return f;
      }
      if (ack) {
        awaiting.put(item.getDeliveryId(), p);
      }
      queue.add(p);
      if (queue.size() >= batchMax) {
        flusher.execute(this::flush);
      } else if (flushScheduled.compareAndSet(false, true)) {
        flusher.schedule(this::flush, lingerNanos, TimeUnit.NANOSECONDS);
      }
      if (closed.get()) {  // closed concurrently: make sure nothing is left waiting
        drainClosed();
      }
      return f;
    }

    void flush() {
      flushScheduled.set(false);
      ClientCallStreamObserver<PushBatch> o = out;
      if (closed.get() || o == null) {
        return;
      }
      if (!o.isReady()) {
        if (flushScheduled.compareAndSet(false, true)) {
          flusher.schedule(this::flush, 1, TimeUnit.MILLISECONDS);
        }
        return;
      }
      while (!queue.isEmpty()) {
        List<Pending> batch = new ArrayList<>(batchMax);
        Pending p;
        while (batch.size() < batchMax && (p = queue.poll()) != null) {
          batch.add(p);
        }
        if (batch.isEmpty()) {
          break;
        }
        PushBatch.Builder b = PushBatch.newBuilder();
        batch.forEach(x -> b.addItems(x.item()));
        try {
          o.onNext(b.build());
        } catch (RuntimeException e) {
          batch.forEach(x -> finish(x, AckItem.Status.STATUS_NACK_CLOSED));
          close();
          return;
        }
        batch.stream().filter(x -> !x.awaitsAck()).forEach(x -> x.future().complete(AckItem.Status.STATUS_WRITTEN));
      }
    }

    @Override
    public void onNext(DeliveryAck acks) {
      for (AckItem a : acks.getAcksList()) {
        Pending p = awaiting.remove(a.getDeliveryId());
        if (p != null) {
          permits.release();
          p.future().complete(a.getStatus());
        }
      }
    }

    @Override
    public void onError(Throwable t) {
      log.info("stream to gateway {} ended: {}", node.id(), t.toString());
      close();
    }

    @Override
    public void onCompleted() {
      log.info("stream to gateway {} completed (drain or shutdown)", node.id());
      close();
    }

    void close() {
      if (closed.compareAndSet(false, true)) {
        ClientCallStreamObserver<PushBatch> o = out;
        if (o != null) {
          try {
            o.onCompleted();
          } catch (RuntimeException ignored) {
            // already broken
          }
        }
      }
      drainClosed();
    }

    void drainClosed() {
      Pending p;
      while ((p = queue.poll()) != null) {
        finish(p, AckItem.Status.STATUS_NACK_CLOSED);
      }
      for (Long id : List.copyOf(awaiting.keySet())) {
        Pending a = awaiting.remove(id);
        if (a != null) {
          finish(a, AckItem.Status.STATUS_NACK_CLOSED);
        }
      }
    }

    private void finish(Pending p, AckItem.Status s) {
      if (p.future().complete(s) && p.awaitsAck()) {
        permits.release();
      }
    }
  }
}
