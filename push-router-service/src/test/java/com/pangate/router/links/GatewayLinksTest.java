package com.pangate.router.links;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.DeliveryAck;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.PushBatch;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.PushStreamGrpc;
import com.pangate.router.nodes.Node;
import com.pangate.router.nodes.NodeDirectory;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GatewayLinksTest {

  final String name = "gw-" + UUID.randomUUID();
  final List<PushBatch> batches = new CopyOnWriteArrayList<>();
  final List<String> versions = new CopyOnWriteArrayList<>();
  final AtomicReference<StreamObserver<DeliveryAck>> toRouter = new AtomicReference<>();
  final Map<String, Node> nodes = new HashMap<>();
  final NodeDirectory dir = new NodeDirectory(now -> nodes, () -> 0L, () -> 0L, Duration.ofSeconds(10));
  Server server;
  GatewayLinks links;

  @BeforeEach
  void start() throws Exception {
    var impl = new PushStreamGrpc.PushStreamImplBase() {
      @Override
      public StreamObserver<PushBatch> stream(StreamObserver<DeliveryAck> out) {
        toRouter.set(out);
        return new StreamObserver<>() {
          @Override public void onNext(PushBatch b) { batches.add(b); }
          @Override public void onError(Throwable t) { }
          @Override public void onCompleted() { }
        };
      }
    };
    ServerInterceptor header = new ServerInterceptor() {
      @Override
      public <Q, A> ServerCall.Listener<Q> interceptCall(ServerCall<Q, A> call, Metadata h, ServerCallHandler<Q, A> next) {
        versions.add(h.get(GatewayLinks.VERSION));
        return next.startCall(call, h);
      }
    };
    server = InProcessServerBuilder.forName(name).directExecutor()
        .addService(ServerInterceptors.intercept(impl, header)).build().start();
    nodes.put("n1", new Node("n1", 1, name, false));
    dir.refresh();
    links = new GatewayLinks(dir, n -> InProcessChannelBuilder.forName(n.address()).directExecutor().build(), 256,
        Duration.ofMillis(2), 2);
  }

  @AfterEach
  void stop() {
    links.close();
    server.shutdownNow();
  }

  static PushItem reliable() {
    return PushItem.newBuilder().setKind(PushItem.Kind.KIND_MESSAGE).setConnId("C1")
        .setFrame(Frame.newBuilder().setClass_(DeliveryClass.DELIVERY_CLASS_RELIABLE)).build();
  }

  static PushItem ephemeral() {
    return PushItem.newBuilder().setKind(PushItem.Kind.KIND_MESSAGE).setConnId("C1")
        .setFrame(Frame.newBuilder().setClass_(DeliveryClass.DELIVERY_CLASS_EPHEMERAL)).build();
  }

  int items() {
    return batches.stream().mapToInt(PushBatch::getItemsCount).sum();
  }

  @Test
  void aFullBatchOf256GoesAtOnceAndTheRestAfterTheLinger() {
    var big = new GatewayLinks(dir, n -> InProcessChannelBuilder.forName(n.address()).directExecutor().build(), 256,
        Duration.ofSeconds(1), 1_000);
    for (int i = 0; i < 300; i++) {
      big.send("n1", ephemeral());
    }
    await().atMost(Duration.ofMillis(500)).until(() -> items() >= 256);  // size trigger, well before the 1 s linger
    assertThat(batches.getFirst().getItemsCount()).isEqualTo(256);
    await().atMost(Duration.ofSeconds(3)).until(() -> items() == 300);   // the rest after the linger
    assertThat(batches).allMatch(b -> b.getItemsCount() <= 256);
    assertThat(versions).containsOnly("1");
    big.close();
  }

  @Test
  void aSingleItemGoesAfterTheShortLinger() {
    long t0 = System.nanoTime();
    links.send("n1", ephemeral()).join();
    assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofMillis(500));
    assertThat(batches).singleElement().satisfies(b -> assertThat(b.getItemsCount()).isEqualTo(1));
  }

  @Test
  void acksCompleteTheMatchingFutures() throws Exception {
    CompletableFuture<AckItem.Status> a = links.send("n1", reliable());
    CompletableFuture<AckItem.Status> b = links.send("n1", reliable());
    await().atMost(Duration.ofSeconds(5)).until(() -> items() == 2);
    var ids = batches.stream().flatMap(x -> x.getItemsList().stream()).map(PushItem::getDeliveryId).toList();
    toRouter.get().onNext(DeliveryAck.newBuilder()
        .addAcks(AckItem.newBuilder().setDeliveryId(ids.get(1)).setStatus(AckItem.Status.STATUS_NACK_TIMEOUT))
        .addAcks(AckItem.newBuilder().setDeliveryId(ids.get(0)).setStatus(AckItem.Status.STATUS_ACKED)).build());
    assertThat(a.get(5, TimeUnit.SECONDS)).isEqualTo(AckItem.Status.STATUS_ACKED);
    assertThat(b.get(5, TimeUnit.SECONDS)).isEqualTo(AckItem.Status.STATUS_NACK_TIMEOUT);
    assertThat(ids.get(0)).isNotEqualTo(ids.get(1));
  }

  @Test
  void streamErrorCompletesPendingWithNackClosed() throws Exception {
    CompletableFuture<AckItem.Status> a = links.send("n1", reliable());
    await().atMost(Duration.ofSeconds(5)).until(() -> items() == 1);
    toRouter.get().onError(Status.UNAVAILABLE.asRuntimeException());
    assertThat(a.get(5, TimeUnit.SECONDS)).isEqualTo(AckItem.Status.STATUS_NACK_CLOSED);
    // the next send opens a new stream
    links.send("n1", ephemeral()).get(5, TimeUnit.SECONDS);
    assertThat(versions).hasSize(2);
  }

  @Test
  void epochChangeClosesTheLinkAndReconnects() throws Exception {      // Review Focus 4
    CompletableFuture<AckItem.Status> a = links.send("n1", reliable());
    await().atMost(Duration.ofSeconds(5)).until(() -> items() == 1);
    nodes.put("n1", new Node("n1", 2, name, false));
    dir.refresh();
    assertThat(a.get(5, TimeUnit.SECONDS)).isEqualTo(AckItem.Status.STATUS_NACK_CLOSED);
    assertThat(links.send("n1", ephemeral()).get(5, TimeUnit.SECONDS)).isEqualTo(AckItem.Status.STATUS_WRITTEN);
    assertThat(versions).hasSize(2);
  }

  @Test
  void maxInFlightBlocksTheSender() throws Exception {
    links.send("n1", reliable());
    links.send("n1", reliable());
    CompletableFuture<CompletableFuture<AckItem.Status>> third =
        CompletableFuture.supplyAsync(() -> links.send("n1", reliable()));
    Thread.sleep(200);
    assertThat(third).isNotDone();
    await().atMost(Duration.ofSeconds(5)).until(() -> items() == 2);
    long first = batches.getFirst().getItems(0).getDeliveryId();
    toRouter.get().onNext(DeliveryAck.newBuilder()
        .addAcks(AckItem.newBuilder().setDeliveryId(first).setStatus(AckItem.Status.STATUS_ACKED)).build());
    third.get(5, TimeUnit.SECONDS);
  }

  @Test
  void ephemeralBroadcastAndControlCompleteWhenWritten() throws Exception {
    assertThat(links.send("n1", ephemeral()).get(5, TimeUnit.SECONDS)).isEqualTo(AckItem.Status.STATUS_WRITTEN);
    assertThat(links.send("n1", PushItem.newBuilder().setKind(PushItem.Kind.KIND_BROADCAST).setTopic("t1:x").build())
        .get(5, TimeUnit.SECONDS)).isEqualTo(AckItem.Status.STATUS_WRITTEN);
    assertThat(links.send("n1", PushItem.newBuilder().setKind(PushItem.Kind.KIND_CONTROL).setPrincipalId("t1:u")
        .build()).get(5, TimeUnit.SECONDS)).isEqualTo(AckItem.Status.STATUS_WRITTEN);
  }

  @Test
  void unknownNodeIsUnavailable() {
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> links.send("nope", ephemeral()))
        .isInstanceOf(NodeUnavailableException.class);
  }
}
