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
}
