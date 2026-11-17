package com.pangate.account.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

class AccountPropertiesTest {

  @Test
  void defaultsMatchSpec() {
    AccountProperties p = new Binder(new MapConfigurationPropertySource(Map.of("account.admin-port", "8081")))
        .bind("account", AccountProperties.class).get();
    assertThat(p.sessionIdleTtl()).isEqualTo(Duration.ofDays(180));
    assertThat(p.lastUsedWriteEvery()).isEqualTo(Duration.ofMinutes(5));
    assertThat(p.maxConcurrentHashes()).isZero();   // 0 = 2 × cores
    assertThat(p.reservedUsernames()).contains("admin", "support", "pangate");
    assertThat(p.auth().baseUrl()).isEqualTo("http://auth:8090");
    assertThat(p.auth().clientId()).isEqualTo("chat-backend");
    assertThat(p.auth().adminTenant()).isEqualTo("chat");
    assertThat(p.kafka().usersTopic()).isEqualTo("account.users");
    assertThat(p.kafka().controlTopic()).isEqualTo("gw.control");
    assertThat(p.redis().timeout()).isEqualTo(Duration.ofMillis(200));
    AccountProperties.Limits l = p.limits();
    assertThat(l.loginIpFailures()).isEqualTo(10);
    assertThat(l.ipFailures()).isEqualTo(50);
    assertThat(l.ipBlockFailures()).isEqualTo(1000);
    assertThat(l.loginAllFailures()).isEqualTo(100);
    assertThat(l.failureWindow()).isEqualTo(Duration.ofMinutes(15));
    assertThat(l.block()).isEqualTo(Duration.ofMinutes(15));
    assertThat(l.loginAllDelay()).isEqualTo(Duration.ofSeconds(2));
    assertThat(l.registrationsPerIpPerHour()).isEqualTo(5);
    assertThat(l.tokensPerSessionPerHour()).isEqualTo(60);
    assertThat(l.lookupsPerUserPerMinute()).isEqualTo(600);
    assertThat(l.adminPerSecond()).isEqualTo(20);
    assertThat(p.outbox().interval()).isEqualTo(Duration.ofMillis(200));
    assertThat(p.outbox().batch()).isEqualTo(500);
    assertThat(p.outbox().keepSent()).isEqualTo(Duration.ofDays(7));
  }
}
