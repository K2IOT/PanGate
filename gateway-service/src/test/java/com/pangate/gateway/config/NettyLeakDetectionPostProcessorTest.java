package com.pangate.gateway.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.util.ResourceLeakDetector;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

class NettyLeakDetectionPostProcessorTest {

  private static final String PROP = "io.netty.leakDetection.level";
  private static final String LEGACY = "io.netty.leakDetectionLevel";
  private ResourceLeakDetector.Level level;
  private String sysProp;

  @BeforeEach
  void save() {
    level = ResourceLeakDetector.getLevel();
    sysProp = System.getProperty(PROP);
  }

  @AfterEach
  void restore() {
    ResourceLeakDetector.setLevel(level);
    if (sysProp == null) {
      System.clearProperty(PROP);
    } else {
      System.setProperty(PROP, sysProp);
    }
    System.clearProperty(LEGACY);
  }

  private static StandardEnvironment env(String value) {
    StandardEnvironment env = new StandardEnvironment();
    env.getPropertySources().addFirst(new MapPropertySource("t", Map.of("gateway.netty.leak-detection", value)));
    return env;
  }

  @Test
  void configDisabledSetsDisabled() {
    System.clearProperty(PROP);
    new NettyLeakDetectionPostProcessor().postProcessEnvironment(env("disabled"), new SpringApplication());
    assertThat(ResourceLeakDetector.getLevel()).isEqualTo(ResourceLeakDetector.Level.DISABLED);
  }

  @Test
  void explicitSystemPropertyWins() {
    System.setProperty(PROP, "paranoid");
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    new NettyLeakDetectionPostProcessor().postProcessEnvironment(env("disabled"), new SpringApplication());
    assertThat(ResourceLeakDetector.getLevel()).isEqualTo(ResourceLeakDetector.Level.PARANOID);
  }

  @Test
  void legacySystemPropertyAlsoWins() {
    System.clearProperty(PROP);
    System.setProperty(LEGACY, "paranoid");
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    new NettyLeakDetectionPostProcessor().postProcessEnvironment(env("disabled"), new SpringApplication());
    assertThat(ResourceLeakDetector.getLevel()).isEqualTo(ResourceLeakDetector.Level.PARANOID);
  }

  @Test
  void invalidLevelNamesTheKeyAndTheAllowedValues() {
    System.clearProperty(PROP);
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> new NettyLeakDetectionPostProcessor()
        .postProcessEnvironment(env("paranoidd"), new SpringApplication()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("gateway.netty.leak-detection")
        .hasMessageContaining("paranoidd")
        .hasMessageContaining("disabled, simple, advanced, paranoid");
  }
}
