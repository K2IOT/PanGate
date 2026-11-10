package com.pangate.auth.support;

import com.pangate.auth.AuthApplication;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** Starts extra application instances ("pods") on random ports against a given database. */
public final class AuthApp {

  private AuthApp() {}

  public static ConfigurableApplicationContext start(String jdbcUrl, String... extra) {
    var pg = Containers.postgres();
    List<String> props = new ArrayList<>(List.of(
        "server.port=0", "management.server.port=0",
        "spring.datasource.url=" + jdbcUrl,
        "spring.datasource.username=" + pg.getUsername(),
        "spring.datasource.password=" + pg.getPassword(),
        "auth.kafka.bootstrap-servers=" + Containers.kafka().getBootstrapServers(),
        "pangate.secrets.dir=" + Containers.secretsDir()));
    props.addAll(List.of(extra));
    return new SpringApplicationBuilder(AuthApplication.class).run(props.stream().map(p -> "--" + p).toArray(String[]::new));
  }

  public static int port(ConfigurableApplicationContext ctx) {
    return Integer.parseInt(ctx.getEnvironment().getProperty("local.server.port"));
  }

  public static int managementPort(ConfigurableApplicationContext ctx) {
    return Integer.parseInt(ctx.getEnvironment().getProperty("local.management.port"));
  }
}
