package com.pangate.devtools.router;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.RefreshConfig;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FakeRouterTest {

  private FakePushStreamServer gw;
  private FakeRouter router;

  @BeforeEach
  void start() throws Exception {
    gw = new FakePushStreamServer();
    router = FakeRouter.connect(gw.target());
  }

  @AfterEach
  void stop() {
    router.close();
    gw.close();
  }

  @Test
  void unicastSendsVersionHeaderAndAwaitsAck() throws Exception {
    long id = router.unicast("CONN", DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", "hi".getBytes());
    AckItem ack = router.awaitAck(id, Duration.ofSeconds(5));
    assertThat(ack.getStatus()).isEqualTo(AckItem.Status.STATUS_ACKED);
    assertThat(gw.versions).containsExactly("1");
    PushItem item = gw.batches.get(0).getItems(0);
    assertThat(item.getKind()).isEqualTo(PushItem.Kind.KIND_MESSAGE);
    assertThat(item.getConnId()).isEqualTo("CONN");
    assertThat(item.getFrame().getMsgId()).isEqualTo("m1");
    assertThat(item.getEnqueuedAtMs()).isPositive();
    assertThat(router.unicast("CONN", DeliveryClass.DELIVERY_CLASS_EPHEMERAL, "m2", new byte[0])).isEqualTo(id + 1);
  }

  @Test
  void awaitAckTimesOut() {
    gw.ack = false;
    long id = router.unicast("CONN", DeliveryClass.DELIVERY_CLASS_RELIABLE, "m1", new byte[0]);
    assertThatThrownBy(() -> router.awaitAck(id, Duration.ofMillis(300))).isInstanceOf(TimeoutException.class);
  }

  @Test
  void broadcastAndControlItems() {
    router.broadcast("t1", "t1:news", PushItem.Priority.PRIORITY_CRITICAL, "b".getBytes());
    router.control("t1", "t1:u1", ControlOp.newBuilder().setRefreshConfig(RefreshConfig.getDefaultInstance()).build());
    await().atMost(Duration.ofSeconds(5)).until(() -> gw.batches.size() == 2);
    PushItem b = gw.batches.get(0).getItems(0);
    assertThat(b.getKind()).isEqualTo(PushItem.Kind.KIND_BROADCAST);
    assertThat(b.getTopic()).isEqualTo("t1:news");
    assertThat(b.getFrame().getTopic()).isEqualTo("t1:news");
    assertThat(b.getPriority()).isEqualTo(PushItem.Priority.PRIORITY_CRITICAL);
    PushItem c = gw.batches.get(1).getItems(0);
    assertThat(c.getKind()).isEqualTo(PushItem.Kind.KIND_CONTROL);
    assertThat(c.getPrincipalId()).isEqualTo("t1:u1");
  }
}
