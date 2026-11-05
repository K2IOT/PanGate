package com.pangate.gateway.config;

import io.netty.util.ResourceLeakDetector;
import java.util.Locale;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * Spec §9.1: Netty leak detection by profile ({@code gateway.netty.leak-detection}: prod disabled, local/dev simple).
 * An explicit {@code -Dio.netty.leakDetection.level} (tests run paranoid) always wins over the config.
 */
public final class NettyLeakDetectionPostProcessor implements EnvironmentPostProcessor, Ordered {

  static final String SYSTEM_PROPERTY = "io.netty.leakDetection.level";

  @Override
  public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
    if (System.getProperty(SYSTEM_PROPERTY) != null) {
      return;
    }
    String level = Binder.get(environment).bind("gateway.netty.leak-detection", String.class).orElse("simple");
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.valueOf(level.trim().toUpperCase(Locale.ROOT)));
  }

  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }
}
