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
  /** Netty still honours its old name; an explicit value under either name wins over the config. */
  static final String LEGACY_SYSTEM_PROPERTY = "io.netty.leakDetectionLevel";
  static final String KEY = "gateway.netty.leak-detection";

  @Override
  public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
    if (System.getProperty(SYSTEM_PROPERTY) != null || System.getProperty(LEGACY_SYSTEM_PROPERTY) != null) {
      return;
    }
    String level = Binder.get(environment).bind(KEY, String.class).orElse("simple");
    ResourceLeakDetector.setLevel(parse(level));
  }

  static ResourceLeakDetector.Level parse(String value) {
    try {
      return ResourceLeakDetector.Level.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(KEY + "=" + value + " is not a leak detection level; use one of: "
          + "disabled, simple, advanced, paranoid", e);
    }
  }

  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }
}
