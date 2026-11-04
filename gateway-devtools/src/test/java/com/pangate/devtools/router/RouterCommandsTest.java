package com.pangate.devtools.router;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.PushItem;
import com.pangate.devtools.Main;
import java.util.List;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class RouterCommandsTest {

  private static int run(String... args) {
    return new CommandLine(new Main()).execute(args);
  }

  @Test
  void fakeRouterUnicastWaitsForAck() throws Exception {
    try (FakePushStreamServer gw = new FakePushStreamServer()) {
      assertThat(run("fake-router", "--target", gw.target(), "unicast", "--conn-id", "C1", "--class", "reliable",
          "--payload", "hi", "--wait-ack", "PT5S")).isZero();
      PushItem i = gw.batches.get(0).getItems(0);
      assertThat(i.getConnId()).isEqualTo("C1");
      assertThat(i.getFrame().getPayload().toStringUtf8()).isEqualTo("hi");
    }
  }

  @Test
  void fakeRouterUnicastWithoutAckFails() throws Exception {
    try (FakePushStreamServer gw = new FakePushStreamServer()) {
      gw.ack = false;
      assertThat(run("fake-router", "--target", gw.target(), "unicast", "--conn-id", "C1", "--class", "reliable",
          "--wait-ack", "PT0.3S")).isEqualTo(1);
    }
  }

  @Test
  void fakeRouterBroadcastAndControl() throws Exception {
    try (FakePushStreamServer gw = new FakePushStreamServer()) {
      assertThat(run("fake-router", "--target", gw.target(), "broadcast", "--tenant", "t1", "--topic", "t1:x",
          "--priority", "critical")).isZero();
      assertThat(run("fake-router", "--target", gw.target(), "control", "--tenant", "t1", "--principal", "t1:u1",
          "--subscribe", "t1:a,t1:b")).isZero();
      assertThat(run("fake-router", "--target", gw.target(), "control", "--tenant", "t1", "--principal", "t1:u1",
          "--kick-new-conn", "NEW", "--connected-at-ms", "9")).isZero();
      org.awaitility.Awaitility.await().until(() -> gw.batches.size() == 3);
      assertThat(gw.batches.get(0).getItems(0).getPriority()).isEqualTo(PushItem.Priority.PRIORITY_CRITICAL);
      assertThat(gw.batches.get(1).getItems(0).getControl().getSubscribe().getTopicsList())
          .containsExactly("t1:a", "t1:b");
      assertThat(gw.batches.get(2).getItems(0).getControl().getKick().getNewConnId()).isEqualTo("NEW");
    }
  }

  @Test
  void healthGrpcServingAndDown() {
    try (FakePushApi api = FakePushApi.start(0, List.of())) {
      assertThat(run("health", "--grpc", "localhost:" + api.port())).isZero();
    }
    assertThat(run("health", "--grpc", "localhost:1")).isEqualTo(1);
  }
}
