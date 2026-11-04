package com.pangate.gateway.downstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.PushBatch;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.RefreshConfig;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.session.ConnId;
import com.pangate.gateway.session.ConnRef;
import com.pangate.gateway.session.PrincipalIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PushDispatcherTest {

  /** Counts tasks handed to the loop. */
  static final class CountingLoop extends DefaultEventLoop {
    final AtomicInteger executes = new AtomicInteger();

    @Override
    public void execute(Runnable task) {
      executes.incrementAndGet();
      super.execute(task);
    }
  }

  record Call(int loop, String what, Object arg) {}

  private final CountingLoop l0 = new CountingLoop();
  private final CountingLoop l1 = new CountingLoop();
  private final LoopRegistry loops = LoopRegistry.fromContexts(List.of(
      new LoopContext(0, l0, 128, () -> 1L), new LoopContext(1, l1, 128, () -> 1L)));
  private final PrincipalIndex principals = new PrincipalIndex();
  private final NodeState node = new NodeState();
  private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  private final List<Call> calls = new CopyOnWriteArrayList<>();
  private final List<PreEncodedFrame> frames = new CopyOnWriteArrayList<>();
  private final AtomicInteger flushes = new AtomicInteger();
  private final EmbeddedChannel written = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
    @Override
    public void flush(ChannelHandlerContext ctx) {
      flushes.incrementAndGet();
      ctx.flush();
    }
  });
  private final List<AckItem> acks = new CopyOnWriteArrayList<>();
  private final StreamRef stream = new StreamRef(acks::add);

  private final LoopItemHandler fake = new LoopItemHandler() {
    @Override
    public Channel message(LoopContext loop, PushItem item, StreamRef s) {
      calls.add(new Call(loop.index(), "message", item.getDeliveryId()));
      return written;
    }

    @Override
    public void broadcast(LoopContext loop, String topic, PushItem.Priority p, PreEncodedFrame frame) {
      calls.add(new Call(loop.index(), "broadcast", topic));
      frames.add(frame);
    }

    @Override
    public void control(LoopContext loop, String principal, ControlOp op) {
      calls.add(new Call(loop.index(), "control", principal));
    }
  };

  private final PushDispatcher dispatcher = new PushDispatcher(loops, principals, node, fake, new GatewayMetrics(meters));

  @AfterEach
  void stop() {
    l0.shutdownGracefully();
    l1.shutdownGracefully();
  }

  private static String conn(int loop) {
    return ConnId.next(System.currentTimeMillis(), loop, ((long) loop << 40) | 5, new Random());
  }

  private static PushItem message(long id, String connId, DeliveryClass cls) {
    return PushItem.newBuilder().setDeliveryId(id).setKind(PushItem.Kind.KIND_MESSAGE).setConnId(connId)
        .setFrame(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setMsgId("m" + id).setClass_(cls)).build();
  }

  private void dispatch(PushItem... items) {
    dispatcher.dispatch(PushBatch.newBuilder().addAllItems(List.of(items)).build(), stream);
  }

  @Test
  void messagesBucketedByConnIdLoopOneTaskPerLoop() {
    DeliveryClass r = DeliveryClass.DELIVERY_CLASS_RELIABLE;
    dispatch(message(1, conn(0), r), message(2, conn(1), r), message(3, conn(0), r), message(4, conn(0), r),
        message(5, conn(1), r));
    await().atMost(Duration.ofSeconds(2)).until(() -> calls.size() == 5);
    assertThat(l0.executes.get()).isEqualTo(1);
    assertThat(l1.executes.get()).isEqualTo(1);
    assertThat(calls.stream().filter(c -> c.loop() == 0).map(Call::arg)).containsExactly(1L, 3L, 4L);
    assertThat(calls.stream().filter(c -> c.loop() == 1).map(Call::arg)).containsExactly(2L, 5L);
  }

  @Test
  void broadcastEncodedOnceSharedByAllLoops() {
    dispatch(PushItem.newBuilder().setKind(PushItem.Kind.KIND_BROADCAST).setTenantId("t1").setTopic("t1:x")
        .setFrame(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setTopic("t1:x")).build());
    await().atMost(Duration.ofSeconds(2)).until(() -> calls.size() == 2);
    assertThat(calls).extracting(Call::loop).containsExactlyInAnyOrder(0, 1);
    assertThat(frames.get(0)).isSameAs(frames.get(1));
    assertThat(frames.get(0).refCnt()).isEqualTo(2);
    frames.get(0).releaseShare();
    frames.get(0).releaseShare();
  }

  @Test
  void broadcastWithoutTopicIsBadItem() {
    dispatch(PushItem.newBuilder().setKind(PushItem.Kind.KIND_BROADCAST).setTenantId("t1")
        .setFrame(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH)).build());
    assertThat(meters.counter("gw.push.dropped", "reason", "bad_item").count()).isEqualTo(1);
    assertThat(l0.executes.get() + l1.executes.get()).isZero();
  }

  @Test
  void controlGoesOnlyToLoopsHoldingPrincipal() {
    principals.add("t1:u1", new ConnRef(1, (1L << 40) | 3));
    principals.add("t1:u1", new ConnRef(1, (1L << 40) | 4));
    ControlOp op = ControlOp.newBuilder().setRefreshConfig(RefreshConfig.getDefaultInstance()).build();
    dispatch(PushItem.newBuilder().setKind(PushItem.Kind.KIND_CONTROL).setPrincipalId("t1:u1").setControl(op).build(),
        PushItem.newBuilder().setKind(PushItem.Kind.KIND_CONTROL).setPrincipalId("t1:nobody").setControl(op).build());
    await().atMost(Duration.ofSeconds(2)).until(() -> calls.size() == 1);
    assertThat(calls).containsExactly(new Call(1, "control", "t1:u1"));
    assertThat(l0.executes.get()).isZero();
  }

  @Test
  void messageToUnknownOrMalformedConnIdIsNackedNotFound() {
    String loop7 = ConnId.next(System.currentTimeMillis(), 7, (7L << 40) | 1, new Random());
    dispatch(message(1, "xyz", DeliveryClass.DELIVERY_CLASS_RELIABLE),
        message(2, loop7, DeliveryClass.DELIVERY_CLASS_ORDERED),
        message(3, "", DeliveryClass.DELIVERY_CLASS_EPHEMERAL));
    assertThat(acks).extracting(AckItem::getDeliveryId, AckItem::getStatus).containsExactly(
        org.assertj.core.groups.Tuple.tuple(1L, AckItem.Status.STATUS_NACK_NOT_FOUND),
        org.assertj.core.groups.Tuple.tuple(2L, AckItem.Status.STATUS_NACK_NOT_FOUND));
    assertThat(meters.counter("gw.push.dropped", "reason", "not_found").count()).isEqualTo(1);
    assertThat(l0.executes.get() + l1.executes.get()).isZero();
  }

  @Test
  void unspecifiedKindIsBadItemAndReliableGetsOneNack() {
    dispatch(PushItem.newBuilder().setDeliveryId(9).setFrame(Frame.newBuilder()
        .setClass_(DeliveryClass.DELIVERY_CLASS_RELIABLE)).build());
    assertThat(acks).extracting(AckItem::getStatus).containsExactly(AckItem.Status.STATUS_NACK_NOT_FOUND);
    assertThat(meters.counter("gw.push.dropped", "reason", "bad_item").count()).isEqualTo(1);
  }

  @Test
  void shedLevel4NacksReliableAndDropsEphemeral() {
    node.shedLevel(4);
    dispatch(message(1, conn(0), DeliveryClass.DELIVERY_CLASS_RELIABLE),
        message(2, conn(0), DeliveryClass.DELIVERY_CLASS_EPHEMERAL));
    assertThat(acks).extracting(AckItem::getStatus).containsExactly(AckItem.Status.STATUS_NACK_SHED);
    assertThat(meters.counter("gw.push.dropped", "reason", "shed").count()).isEqualTo(1);
    assertThat(l0.executes.get()).isZero();
  }

  @Test
  void writtenChannelsFlushedOncePerTask() {
    DeliveryClass r = DeliveryClass.DELIVERY_CLASS_RELIABLE;
    dispatch(message(1, conn(0), r), message(2, conn(0), r), message(3, conn(0), r));
    await().atMost(Duration.ofSeconds(2)).until(() -> flushes.get() >= 1);
    assertThat(calls).hasSize(3);
    assertThat(flushes.get()).isEqualTo(1);
  }
}
