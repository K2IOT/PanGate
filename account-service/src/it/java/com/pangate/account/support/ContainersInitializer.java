package com.pangate.account.support;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Points the datasource, Kafka and Redis at {@link Containers}, and auth at {@link AuthStack}. A value a test already
 * set (e.g. {@code @TestPropertySource} with a dead Redis) is kept: TestPropertyValues would otherwise win over it.
 */
public final class ContainersInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  @Override
  public void initialize(ConfigurableApplicationContext ctx) {
    var pg = Containers.postgres();
    Map<String, String> values = new LinkedHashMap<>();
    values.put("spring.datasource.url", pg.getJdbcUrl());
    values.put("spring.datasource.username", pg.getUsername());
    values.put("spring.datasource.password", pg.getPassword());
    values.put("account.kafka.bootstrap", Containers.kafka().getBootstrapServers());
    values.put("account.redis.uri", Containers.redisUri());
    values.put("account.auth.base-url", AuthStack.baseUrl());
    values.put("account.auth.issuer", AuthStack.baseUrl());
    values.put("account.auth.jwks-uri", AuthStack.baseUrl() + "/.well-known/jwks.json");
    values.put("account.auth.client-file", AuthStack.clientsFile().toString());
    var env = ctx.getEnvironment();
    values.keySet().removeIf(k -> env.getPropertySources().stream()
        .anyMatch(s -> s.getName().contains("Inlined Test Properties") && s.containsProperty(k)));
    TestPropertyValues.of(values).applyTo(env);
  }
}
