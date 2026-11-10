package com.pangate.auth.support;

import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

/** Points the datasource and Kafka at {@link Containers}. */
public final class ContainersInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  @Override
  public void initialize(ConfigurableApplicationContext ctx) {
    var pg = Containers.postgres();
    TestPropertyValues.of(
        "spring.datasource.url=" + pg.getJdbcUrl(),
        "spring.datasource.username=" + pg.getUsername(),
        "spring.datasource.password=" + pg.getPassword(),
        "auth.kafka.bootstrap-servers=" + Containers.kafka().getBootstrapServers())
        .applyTo(ctx.getEnvironment());
  }
}
