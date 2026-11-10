package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class PushPathScenarioIT {

  @RegisterExtension
  static PushPathScenario scenario = new PushPathScenario("c1");

  static FakeRouter router;

  @BeforeAll
  static void startRouter() {
    router = new FakeRouter(scenario.bootstrapServers(), scenario.cell());
  }

  @AfterAll
  static void stopRouter() throws InterruptedException {
    router.close();
  }

  private static String id() {
    return UUID.randomUUID().toString();
  }

  private static String principal() {
    return "t1:u" + UUID.randomUUID().toString().substring(0, 8);
  }

  @Test
  void messageGoesOfflineWithConventions() {
    PushRecord m = Fixtures.withMsgId(Fixtures.message(principal()), id());
    scenario.produce(m);
    OfflineRecord o = scenario.awaitOffline(m.getMsgId(), OfflineRecord.Reason.REASON_NO_SESSION);
    assertThat(o.getRecord()).isEqualTo(m);
  }

  @Test
  void receiptProducesADeliveredRecord() {
    PushRecord m = Fixtures.withMsgId(Fixtures.message(principal()), id()).toBuilder().setReceipt(true)
        .setSenderPrincipalId("t1:sender").build();
    scenario.produce(m);
    assertThat(scenario.awaitDelivered(m.getMsgId()).getSenderPrincipalId()).isEqualTo("t1:sender");
  }

  @Test
  void recordsOfOnePrincipalKeepTheirOrder() {
    String p = principal();
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      PushRecord m = Fixtures.withMsgId(Fixtures.message(p), id());
      ids.add(m.getMsgId());
      scenario.produce(m);
    }
    var mine = scenario.drain(PushTopics.requests("c1").name()).stream().filter(r -> r.key().equals(p)).toList();
    assertThat(mine).extracting(r -> PushRecordParser.msgId(r.value())).containsExactlyElementsOf(ids);
    assertThat(mine).extracting(r -> r.partition()).containsOnly(mine.get(0).partition());
  }

  @Test
  void broadcastIsKeyedByTopic() {
    String topic = "t1:news-" + UUID.randomUUID().toString().substring(0, 6);
    PushRecord b = Fixtures.withMsgId(Fixtures.broadcast(topic), id());
    scenario.produce(b);
    assertThat(scenario.awaitRequest(b.getMsgId())).isEqualTo(b);
    assertThat(scenario.drain(PushTopics.requests("c1").name()))
        .anySatisfy(r -> assertThat(r.key()).isEqualTo("topic:" + topic));
  }

  @Test
  void nodeDownCanBeAwaited() {
    ConnectionEvent e = Fixtures.nodeDown("gw-" + UUID.randomUUID().toString().substring(0, 6), 9);
    scenario.produce(PushTopics.lifecycle("c1").name(), PushKeys.of(e), e, "ConnectionEvent", "");
    assertThat(scenario.awaitNodeDown(e.getNodeId(), 9)).isEqualTo(e);
  }

  /** Reads only the msg_id field so the order test does not depend on the parsing path under test. */
  static final class PushRecordParser {
    static String msgId(byte[] v) {
      try {
        return PushRecord.parseFrom(v).getMsgId();
      } catch (Exception e) {
        return "";
      }
    }
  }
}
