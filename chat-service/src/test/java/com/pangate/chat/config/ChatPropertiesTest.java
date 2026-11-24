package com.pangate.chat.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

class ChatPropertiesTest {

  @Test
  void defaultsMatchSpec() {
    ChatProperties p = new Binder(new MapConfigurationPropertySource(Map.of("chat.cell", "c1")))
        .bind("chat", ChatProperties.class).get();
    assertThat(p.cell()).isEqualTo("c1");
    // CH3 spec §3.3, §3.4, §5, §6
    assertThat(p.state().markRate()).isEqualTo(50);
    assertThat(p.state().markBurst()).isEqualTo(100);
    assertThat(p.state().dmWindow()).isEqualTo(java.time.Duration.ofMillis(500));
    assertThat(p.state().groupWindow()).isEqualTo(java.time.Duration.ofSeconds(2));
    assertThat(p.state().listedReceiptsMax()).isEqualTo(50);
    assertThat(p.state().offlineGrace()).isEqualTo(java.time.Duration.ofSeconds(5));
    assertThat(p.state().watchTtl()).isEqualTo(java.time.Duration.ofMinutes(5));
    assertThat(p.state().watchMax()).isEqualTo(200);
    assertThat(p.state().typingEvery()).isEqualTo(java.time.Duration.ofSeconds(2));
    assertThat(p.kafka().receiptsTopic()).isEqualTo("chat.receipts");
    assertThat(p.kafka().commandsTopic()).isEqualTo("chat.commands");
    assertThat(p.kafka().eventsTopic()).isEqualTo("chat.events");
    assertThat(p.kafka().dlqTopic()).isEqualTo("chat.commands.dlq");
    assertThat(p.kafka().upstreamTopic()).isEqualTo("c1.gw.upstream.cmd");
    assertThat(p.limits().userRate()).isEqualTo(20);
    assertThat(p.limits().userBurst()).isEqualTo(40);
    assertThat(p.limits().convRate()).isEqualTo(50);
    assertThat(p.limits().convBurst()).isEqualTo(100);
    assertThat(p.limits().maxGroupMembers()).isEqualTo(500);
    assertThat(p.limits().maxTextBytes()).isEqualTo(4096);
    assertThat(p.limits().maxMentions()).isEqualTo(50);
    assertThat(p.scylla().keyspace()).isEqualTo("chat");
    assertThat(p.scylla().timeout()).isEqualTo(Duration.ofSeconds(1));
    assertThat(p.scylla().contactPoints()).isEqualTo(List.of("scylla:9042"));
    assertThat(p.http().sendWait()).isEqualTo(Duration.ofSeconds(3));
    assertThat(p.auth().audience()).isEqualTo("pangate-gw");
    assertThat(p.redis().memberCacheTtl()).isEqualTo(Duration.ofSeconds(60));
    assertThat(p.managementPort()).isEqualTo(8081);
  }

  /** Perf 3c (spec §4.6). */
  @Test
  void pipelineDefaultsMatchSpec() {
    ChatProperties p = new Binder(new MapConfigurationPropertySource(Map.of("chat.cell", "c1")))
        .bind("chat", ChatProperties.class).get();
    assertThat(p.pipeline().maxPendingPerPartition()).isEqualTo(1000);
    assertThat(p.pipeline().maxBatch()).isEqualTo(100);
    assertThat(p.pipeline().maxConcurrency()).isEqualTo(256);
    assertThat(p.pipeline().commitInterval()).isEqualTo(Duration.ofMillis(200));
    assertThat(p.pipeline().revokeWait()).isEqualTo(Duration.ofSeconds(5));
    assertThat(p.fanout().inboxConcurrency()).isEqualTo(256);
    assertThat(p.kafka().stateGroup()).isEqualTo("chat-state");
  }
}
