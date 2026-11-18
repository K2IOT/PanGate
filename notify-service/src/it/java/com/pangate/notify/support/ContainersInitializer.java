package com.pangate.notify.support;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

/** Points the datasource, Kafka and Redis at {@link Containers} and auth at {@link FakeAuth}. */
public final class ContainersInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  @Override
  public void initialize(ConfigurableApplicationContext ctx) {
    var pg = Containers.postgres();
    Map<String, String> values = new LinkedHashMap<>();
    values.put("spring.datasource.url", pg.getJdbcUrl());
    values.put("spring.datasource.username", pg.getUsername());
    values.put("spring.datasource.password", pg.getPassword());
    values.put("notify.kafka.bootstrap", Containers.kafka().getBootstrapServers());
    values.put("notify.redis.uri", Containers.redisUri());
    values.put("notify.auth.issuer", FakeAuth.ISSUER);
    values.put("notify.auth.jwks-uri", FakeAuth.jwksUri());
    var env = ctx.getEnvironment();
    values.keySet().removeIf(k -> env.getPropertySources().stream()
        .anyMatch(s -> s.getName().contains("Inlined Test Properties") && s.containsProperty(k)));
    TestPropertyValues.of(values).applyTo(env);
  }
}
