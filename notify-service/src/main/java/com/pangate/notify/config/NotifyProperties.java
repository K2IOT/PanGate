package com.pangate.notify.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** {@code notify.*} (offline notifications spec §5). */
@ConfigurationProperties("notify")
public record NotifyProperties(
    @DefaultValue("8081") int adminPort,
    /** Tokens not registered again for this long are purged (spec §5.1: the session idle TTL of CH2). */
    @DefaultValue("180d") Duration tokenIdleTtl,
    @DefaultValue("10") int registrationsPerMinute,
    @DefaultValue Auth auth,
    @DefaultValue Kafka kafka,
    @DefaultValue Redis redis) {

  public record Auth(
      @DefaultValue("http://auth:8090") String issuer,
      @DefaultValue("http://auth:8090/.well-known/jwks.json") String jwksUri) {}

  public record Kafka(
      @DefaultValue("kafka:9092") String bootstrap,
      @DefaultValue("c1.notify.requests") String requestsTopic,
      @DefaultValue("gw.control") String controlTopic) {}

  public record Redis(
      @DefaultValue("redis://redis:6379") String uri,
      @DefaultValue("200ms") Duration timeout) {}
}
