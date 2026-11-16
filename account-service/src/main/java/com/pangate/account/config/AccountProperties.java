package com.pangate.account.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * {@code account.*} (account spec §2, §7). Limits are the spec §7 table; {@code maxConcurrentHashes = 0} means
 * 2 × available processors.
 */
@ConfigurationProperties("account")
public record AccountProperties(
    @DefaultValue("180d") Duration sessionIdleTtl,
    @DefaultValue("5m") Duration lastUsedWriteEvery,
    @DefaultValue("0") int maxConcurrentHashes,
    @DefaultValue({"admin", "administrator", "support", "pangate", "system", "root", "chat", "official"})
    List<String> reservedUsernames,
    @DefaultValue("8081") int adminPort,
    @DefaultValue Auth auth,
    @DefaultValue Kafka kafka,
    @DefaultValue Redis redis,
    @DefaultValue Limits limits,
    @DefaultValue Outbox outbox) {

  /** auth-service: client credentials of {@code clientId}, JWKS for connect and API tokens. */
  public record Auth(
      @DefaultValue("http://auth:8090") String baseUrl,
      @DefaultValue("http://auth:8090") String issuer,
      @DefaultValue("http://auth:8090/.well-known/jwks.json") String jwksUri,
      @DefaultValue("chat-backend") String clientId,
      @DefaultValue("account/chat-backend-secret") String clientSecretName,
      Path clientFile,
      @DefaultValue("chat") String adminTenant,
      @DefaultValue("2s") Duration timeout) {}

  public record Kafka(
      @DefaultValue("kafka:9092") String bootstrap,
      @DefaultValue("account.users") String usersTopic,
      @DefaultValue("gw.control") String controlTopic) {}

  public record Redis(
      @DefaultValue("redis://redis:6379") String uri,
      @DefaultValue("200ms") Duration timeout) {}

  public record Limits(
      @DefaultValue("10") int loginIpFailures,
      @DefaultValue("50") int ipFailures,
      @DefaultValue("100") int loginAllFailures,
      @DefaultValue("15m") Duration failureWindow,
      @DefaultValue("15m") Duration block,
      @DefaultValue("2s") Duration loginAllDelay,
      @DefaultValue("5") int registrationsPerIpPerHour,
      @DefaultValue("60") int tokensPerSessionPerHour,
      @DefaultValue("600") int lookupsPerUserPerMinute,
      @DefaultValue("20") int adminPerSecond) {}

  public record Outbox(
      @DefaultValue("200ms") Duration interval,
      @DefaultValue("500") int batch,
      @DefaultValue("7d") Duration keepSent) {}
}
