package com.pangate.router;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Configuration lives in {@code router.yml} / {@code router-<profile>.yml}, not {@code application.yml}: RouterIT runs
 * a real gateway in the same JVM, whose {@code application.yml} must stay the only one on the classpath.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class RouterApplication {

  public static final String CONFIG_NAME = "spring.config.name=router";

  public static void main(String[] args) {
    new SpringApplicationBuilder(RouterApplication.class).properties(CONFIG_NAME).run(args);
  }
}
