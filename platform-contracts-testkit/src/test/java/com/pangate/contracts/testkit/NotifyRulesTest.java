package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.NotifyRequest;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** CH4 spec §2.1: what a NotifyRequest must hold, its key and its topic. */
class NotifyRulesTest {

  @Test
  void validCasesHaveNoViolations() {
    assertThat(NotifyCases.valid()).isNotEmpty();
    for (NotifyCases.Case c : NotifyCases.valid()) {
      assertThat(NotifyRules.violations(c.request())).as(c.name()).isEmpty();
    }
  }

  @Test
  void eachInvalidCaseBreaksItsRule() {
    for (NotifyCases.Case c : NotifyCases.invalid()) {
      assertThat(NotifyRules.violations(c.request())).as(c.name()).contains(c.rule());
    }
    Set<String> rules = NotifyCases.invalid().stream().map(NotifyCases.Case::rule).collect(Collectors.toSet());
    assertThat(rules).contains("request_id", "target", "expires", "alert", "alert_size");
  }

  @Test
  void keyIsTheRecipient() {
    NotifyRequest toDevice = NotifyCases.valid().get(0).request().toBuilder()
        .setDevice(DeviceRef.newBuilder().setPrincipalId("p1").setDeviceId("d1")).build();
    assertThat(NotifyKeys.of(toDevice)).isEqualTo("p1");
    NotifyRequest toPrincipal = toDevice.toBuilder().setPrincipalId("p2").build();
    assertThat(NotifyKeys.of(toPrincipal)).isEqualTo("p2");
  }

  @Test
  void topicIsSizedAsTheSpecSays() {
    assertThat(NotifyTopics.requests("c1").name()).isEqualTo("c1.notify.requests");
    assertThat(NotifyTopics.requests("c1").partitions()).isEqualTo(32);
    assertThat(NotifyTopics.requests("c1").retention()).isEqualTo(Duration.ofDays(1));
    assertThat(ChatTopics.CHANNEL_NOTIFY).isEqualTo("chat.channel.notify");
  }
}
