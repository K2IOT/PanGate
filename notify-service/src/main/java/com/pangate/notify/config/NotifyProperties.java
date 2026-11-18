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
    @DefaultValue Redis redis,
    @DefaultValue("1000") int maxInFlight,
    /** Tries per token, the first included (spec §5.4). */
    @DefaultValue("3") int attempts,
    /** Generate missing key files at start-up (devenv only, spec §7). */
    @DefaultValue("false") boolean devCredentials,
    java.util.Map<String, Tenant> tenants) {

  public java.util.Map<String, Tenant> tenants() {
    return tenants == null ? java.util.Map.of() : tenants;
  }

  /** APNs and FCM credentials of one tenant's app; a platform left out is not sent to (spec §5.4). */
  public record Tenant(Apns apns, Fcm fcm) {}

  public record Apns(
      @DefaultValue("https://api.push.apple.com") String baseUrl,
      @DefaultValue("https://api.sandbox.push.apple.com") String sandboxBaseUrl,
      java.nio.file.Path keyFile,
      String keyId,
      String teamId,
      String bundleId) {}

  public record Fcm(
      @DefaultValue("https://fcm.googleapis.com") String baseUrl,
      /** Overrides the service account's token_uri (the fake provider's in devenv). */
      String tokenUri,
      String projectId,
      java.nio.file.Path serviceAccountFile) {}

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
