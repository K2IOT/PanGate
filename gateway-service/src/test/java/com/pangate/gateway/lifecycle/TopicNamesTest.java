package com.pangate.gateway.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;

class TopicNamesTest {

  private final TopicNames names = new TopicNames("c1", List.of("telemetry", "cmd", "cmd_result"));

  @Test
  void namesFollowPrefix() {
    assertThat(names.upstream("cmd")).isEqualTo("c1.gw.upstream.cmd");
    assertThat(names.lifecycle()).isEqualTo("c1.gw.lifecycle");
    assertThat(TopicNames.CONTROL).isEqualTo("gw.control");
    assertThat(names.isUpstreamType("cmd")).isTrue();
    assertThat(names.isUpstreamType("read")).isFalse();
  }

  @Test
  void allHasPartitionCountsAndCompactControl() {
    Map<String, NewTopic> all = names.all().stream().collect(Collectors.toMap(NewTopic::name, t -> t));

    assertThat(all).containsOnlyKeys("c1.gw.lifecycle", "c1.gw.upstream.telemetry", "c1.gw.upstream.cmd",
        "c1.gw.upstream.cmd_result", "gw.control");
    assertThat(all.get("c1.gw.lifecycle").numPartitions()).isEqualTo(32);
    assertThat(all.get("c1.gw.upstream.cmd").numPartitions()).isEqualTo(64);
    assertThat(all.get("gw.control").numPartitions()).isEqualTo(8);
    assertThat(all.get("gw.control").configs()).containsEntry("cleanup.policy", "compact");
    assertThat(all.get("c1.gw.lifecycle").replicationFactor()).isEqualTo((short) -1);
  }

  @Test
  void onlyCmdAndCmdResultAreReliable() {
    assertThat(TopicNames.isReliable("cmd")).isTrue();
    assertThat(TopicNames.isReliable("cmd_result")).isTrue();
    assertThat(TopicNames.isReliable("telemetry")).isFalse();
  }
}
