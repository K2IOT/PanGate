package com.pangate.account.support;

import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

/** Points the datasource, Kafka and Redis at {@link Containers}. */
public final class ContainersInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  @Override
  public void initialize(ConfigurableApplicationContext ctx) {
    var pg = Containers.postgres();
    TestPropertyValues.of(
        "spring.datasource.url=" + pg.getJdbcUrl(),
        "spring.datasource.username=" + pg.getUsername(),
        "spring.datasource.password=" + pg.getPassword(),
        "account.kafka.bootstrap=" + Containers.kafka().getBootstrapServers(),
        "account.redis.uri=" + Containers.redisUri())
        .applyTo(ctx.getEnvironment());
  }
}
