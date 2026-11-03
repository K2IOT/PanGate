package com.pangate.starter.autoconfigure;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/** Common metric tags: cell, az, node, service (empty values are skipped). */
@AutoConfiguration
@ConditionalOnClass({MeterRegistry.class, MeterRegistryCustomizer.class})
@EnableConfigurationProperties(PangateProperties.class)
public class PangateObservabilityAutoConfiguration {

  @Bean
  MeterRegistryCustomizer<MeterRegistry> pangateCommonTags(PangateProperties props) {
    List<Tag> tags = new ArrayList<>();
    add(tags, "cell", props.cell());
    add(tags, "az", props.az());
    add(tags, "node", props.node());
    add(tags, "service", props.service());
    return registry -> registry.config().commonTags(tags);
  }

  private static void add(List<Tag> tags, String key, String value) {
    if (value != null && !value.isBlank()) {
      tags.add(Tag.of(key, value));
    }
  }
}
