package com.pangate.pushapi.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.unit.DataSize;

class PushApiPropertiesTest {

  static PushApiProperties bind(Iterable<PropertySource<?>> sources) {
    return new Binder(ConfigurationPropertySources.from(sources)).bind("push-api", PushApiProperties.class)
        .orElseGet(() -> new Binder(new MapConfigurationPropertySource(Map.of("push-api.cell", "c1")))
            .bind("push-api", PushApiProperties.class).get());
  }

  @Test
  void defaultsMatchSpec() {
    PushApiProperties p = new Binder(new MapConfigurationPropertySource(Map.of("push-api.cell", "c1")))
        .bind("push-api", PushApiProperties.class).get();
    assertThat(p.cell()).isEqualTo("c1");
    assertThat(p.kafka().produceTimeout()).isEqualTo(Duration.ofSeconds(5));
    assertThat(p.kafka().statusGroup()).isEqualTo("push-api-status");
    assertThat(p.redis().timeout()).isEqualTo(Duration.ofMillis(200));
    assertThat(p.redis().topology()).isEqualTo("cluster");
    assertThat(p.auth().audience()).isEqualTo("pangate-api");
    assertThat(p.auth().issuer()).isEqualTo("https://auth.pangate");
    assertThat(p.limits().pushRate()).isEqualTo(1000);
    assertThat(p.limits().pushBurst()).isEqualTo(2000);
    assertThat(p.limits().broadcastRate()).isEqualTo(10);
    assertThat(p.limits().statusRate()).isEqualTo(100);
    assertThat(p.ttl().defaultTtl()).isEqualTo(Duration.ofSeconds(86_400));
    assertThat(p.ttl().max()).isEqualTo(Duration.ofSeconds(604_800));
    assertThat(p.idempotency().ttl()).isEqualTo(Duration.ofHours(1));
    assertThat(p.idempotency().pendingTtl()).isEqualTo(Duration.ofSeconds(30));
    assertThat(p.idempotency().waitFor()).isEqualTo(Duration.ofSeconds(2));
    assertThat(p.seqepochs().refresh()).isEqualTo(Duration.ofSeconds(5));
    assertThat(p.grpc().port()).isEqualTo(9443);
    assertThat(p.grpc().plaintext()).isFalse();
    assertThat(p.grpc().maxInFlightPerStream()).isEqualTo(1000);
    assertThat(p.grpc().allowedSpiffe())
        .containsExactly("spiffe://pangate/cell-1/gateway", "spiffe://pangate/cell-1/push-router");
    assertThat(p.http().maxBody()).isEqualTo(DataSize.ofKilobytes(128));
    assertThat(p.http().maxBatch()).isEqualTo(100);
    assertThat(p.http().maxBatchBody()).isEqualTo(DataSize.ofMegabytes(1));
  }

  @Test
  void localProfileOverrides() throws Exception {
    YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
    List<PropertySource<?>> sources = new java.util.ArrayList<>();
    sources.addAll(loader.load("local", new ClassPathResource("push-api-local.yml")));
    sources.addAll(loader.load("base", new ClassPathResource("push-api.yml")));
    PushApiProperties p = bind(sources);
    Binder b = new Binder(ConfigurationPropertySources.from(sources));
    assertThat(b.bind("server.port", Integer.class).get()).isEqualTo(8085);
    assertThat(b.bind("push-api.management-port", Integer.class).get()).isEqualTo(8084);
    assertThat(p.grpc().plaintext()).isTrue();
    assertThat(p.redis().uri()).isEqualTo("redis://localhost:6379");
    assertThat(p.redis().topology()).isEqualTo("standalone");
    assertThat(p.auth().issuer()).isEqualTo("http://localhost:8090");
    assertThat(p.auth().jwksUri()).isEqualTo("http://localhost:8090/.well-known/jwks.json");
    assertThat(p.kafka().bootstrap()).isEqualTo("localhost:9092");
  }

  /** Final review minor: spec §5 drains in 10 s per phase, inside the compose stop_grace_period of 20 s. */
  @Test
  void shutdownPhaseTimeoutIsTenSeconds() {
    var yaml = new org.springframework.beans.factory.config.YamlPropertiesFactoryBean();
    yaml.setResources(new org.springframework.core.io.ClassPathResource("push-api.yml"));
    assertThat(yaml.getObject().getProperty("spring.lifecycle.timeout-per-shutdown-phase")).isEqualTo("10s");
  }
}
