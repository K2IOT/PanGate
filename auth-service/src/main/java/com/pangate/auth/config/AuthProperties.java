package com.pangate.auth.config;

import java.nio.file.Path;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Settings under {@code auth.*}; defaults are spec §8. */
@ConfigurationProperties("auth")
public record AuthProperties(
    @DefaultValue("https://auth.pangate") String issuer,
    @DefaultValue("15m") Duration apiTokenTtl,
    @DefaultValue("10m") Duration connectTokenMaxTtl,
    @DefaultValue("24h") Duration revocationTtl,
    @DefaultValue Keys keys,
    @DefaultValue Outbox outbox,
    @DefaultValue Kafka kafka,
    @DefaultValue Seed seed) {

  public record Keys(
      @DefaultValue("30d") Duration lifetime,
      @DefaultValue("7d") Duration overlap,
      @DefaultValue("1h") Duration publishAhead,
      @DefaultValue("v1") String kekId,
      @DefaultValue("60s") Duration cacheRefresh,
      @DefaultValue("1h") Duration rotateInterval) {}

  public record Outbox(
      @DefaultValue("200ms") Duration interval,
      @DefaultValue("500") int batch,
      @DefaultValue("7d") Duration keepSent) {}

  public record Kafka(
      @DefaultValue("gw.control") String controlTopic,
      @DefaultValue("kafka:9092") String bootstrapServers) {}

  public record Seed(@DefaultValue("false") boolean enabled, @DefaultValue(".dev/auth") Path dir) {}
}
