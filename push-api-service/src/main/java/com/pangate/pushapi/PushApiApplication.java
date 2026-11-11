package com.pangate.pushapi;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Configuration lives in {@code push-api.yml} / {@code push-api-<profile>.yml}: PushApiIT runs the gateway and the
 * router in the same JVM, whose {@code application.yml} / {@code router.yml} must not be shadowed.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class PushApiApplication {

  public static final String CONFIG_NAME = "spring.config.name=push-api";

  public static void main(String[] args) {
    new SpringApplicationBuilder(PushApiApplication.class).properties(CONFIG_NAME).run(args);
  }
}
