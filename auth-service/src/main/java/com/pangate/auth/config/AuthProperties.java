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
    @DefaultValue("60s") Duration expiryInterval,
    @DefaultValue Keys keys,
    @DefaultValue Outbox outbox,
    @DefaultValue Kafka kafka,
    @DefaultValue Seed seed,
    @DefaultValue RateLimits rateLimits) {

  public record Keys(
      @DefaultValue("30d") Duration lifetime,
      @DefaultValue("7d") Duration overlap,
      @DefaultValue("1h") Duration publishAhead,
      @DefaultValue("v1") String kekId,
      @DefaultValue("60s") Duration cacheRefresh,
      @DefaultValue("1h") Duration rotateInterval) {
    public com.pangate.auth.domain.KeyPolicy policy() {
      return new com.pangate.auth.domain.KeyPolicy(lifetime, overlap, publishAhead);
    }
  }

  public record Outbox(
      @DefaultValue("200ms") Duration interval,
      @DefaultValue("500") int batch,
      @DefaultValue("7d") Duration keepSent) {}

  public record Kafka(
      @DefaultValue("gw.control") String controlTopic,
      @DefaultValue("kafka:9092") String bootstrapServers) {}

  /** A zero rate means "spec default"; see {@link RateLimits}. */
  public record Limit(@DefaultValue("0") double rate, @DefaultValue("0") int burst) {
    Limit or(double r, int b) {
      return rate > 0 ? new Limit(rate, burst > 0 ? burst : (int) Math.ceil(rate)) : new Limit(r, b);
    }
  }

  public record AuthFailures(@DefaultValue("20") int perMinute, @DefaultValue("60s") Duration block) {}

  /** Spec §7 defaults: token 10/s burst 20, mint 1000/s burst 2000, revoke 100/s burst 200, admin 20/s. */
  public record RateLimits(@DefaultValue Limit token, @DefaultValue Limit mint, @DefaultValue Limit revoke,
      @DefaultValue Limit admin, @DefaultValue AuthFailures authFailures) {
    public RateLimits {
      token = token.or(10, 20);
      mint = mint.or(1000, 2000);
      revoke = revoke.or(100, 200);
      admin = admin.or(20, 20);
    }
  }

  public record Seed(@DefaultValue("false") boolean enabled, @DefaultValue(".dev/auth") Path dir) {}
}
