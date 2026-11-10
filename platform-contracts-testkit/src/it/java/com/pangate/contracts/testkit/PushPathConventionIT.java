package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Convention checks fail loudly; on its own cell so the broken records never reach other scenario tests. */
class PushPathConventionIT {

  @RegisterExtension
  static PushPathScenario scenario = new PushPathScenario("cbad");

  @Test
  void conventionViolationFailsTheAwaitAndTheDrain() {
    PushRecord m = Fixtures.withMsgId(Fixtures.message("t1:u1"), UUID.randomUUID().toString());
    RecordHeaders noCell = new RecordHeaders();
    noCell.add("tenant-id", "t1".getBytes());
    noCell.add("content-type", PushHeaders.contentType("PushRecord").getBytes());
    scenario.produceRaw(PushTopics.requests("cbad").name(), PushKeys.of(m), m.toByteArray(), noCell);
    assertThatThrownBy(() -> scenario.awaitRequest(m.getMsgId(), Duration.ofSeconds(20)))
        .isInstanceOf(AssertionError.class).hasMessageContaining("cell-id");
    assertThatThrownBy(() -> scenario.drain(PushTopics.requests("cbad").name()))
        .isInstanceOf(AssertionError.class).hasMessageContaining("cell-id");
  }

  @Test
  void awaitOfflineNamesTheReasonItSaw() {
    PushRecord m = Fixtures.withMsgId(Fixtures.message("t1:u2"), UUID.randomUUID().toString());
    scenario.produce(PushTopics.offline("cbad").name(), PushKeys.of(m),
        Fixtures.offline(m, OfflineRecord.Reason.REASON_SHED, 1), "OfflineRecord", "t1");
    assertThatThrownBy(() -> scenario.awaitOffline(m.getMsgId(), OfflineRecord.Reason.REASON_NO_SESSION,
        Duration.ofSeconds(5))).isInstanceOf(AssertionError.class).hasMessageContaining("REASON_SHED");
    assertThat(scenario.awaitOffline(m.getMsgId(), OfflineRecord.Reason.REASON_SHED).getAttempts()).isEqualTo(1);
  }
}
