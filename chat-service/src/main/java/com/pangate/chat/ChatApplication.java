package com.pangate.chat;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Configuration lives in {@code chat.yml} / {@code chat-<profile>.yml}: the end-to-end IT runs Push API, the router
 * and gateways in the same JVM, whose own configuration files must not be shadowed. The ScyllaDB driver shares the
 * Cassandra driver's packages, so Spring Boot's Cassandra auto-configuration is off: the session is built here.
 */
@SpringBootApplication(exclude = CassandraAutoConfiguration.class)
@ConfigurationPropertiesScan
public class ChatApplication {

  public static final String CONFIG_NAME = "spring.config.name=chat";

  public static void main(String[] args) {
    new SpringApplicationBuilder(ChatApplication.class).properties(CONFIG_NAME).run(args);
  }
}
