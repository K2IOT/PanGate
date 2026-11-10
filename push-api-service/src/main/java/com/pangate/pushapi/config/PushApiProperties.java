package com.pangate.pushapi.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/** {@code push-api.*}, defaults of push-api spec §6. */
@ConfigurationProperties("push-api")
public record PushApiProperties(
    @DefaultValue("c1") String cell,
    @DefaultValue("8081") int managementPort,
    @DefaultValue Kafka kafka,
    @DefaultValue Redis redis,
    @DefaultValue Auth auth,
    @DefaultValue Limits limits,
    @DefaultValue Ttl ttl,
    @DefaultValue Idempotency idempotency,
    @DefaultValue SeqEpochs seqepochs,
    @DefaultValue Grpc grpc,
    @DefaultValue Http http) {

  public record Kafka(@DefaultValue("kafka:9092") String bootstrap, @DefaultValue("5s") Duration produceTimeout,
      @DefaultValue("push-api-status") String statusGroup) {}

  public record Redis(@DefaultValue("redis://redis-state:6379") String uri, @DefaultValue("cluster") String topology,
      @DefaultValue("200ms") Duration timeout) {}

  public record Auth(@DefaultValue("https://auth.pangate/.well-known/jwks.json") String jwksUri,
      @DefaultValue("https://auth.pangate") String issuer, @DefaultValue("pangate-api") String audience) {}

  public record Limits(@DefaultValue("1000") int pushRate, @DefaultValue("2000") int pushBurst,
      @DefaultValue("10") int broadcastRate, @DefaultValue("100") int statusRate) {}

  public record Ttl(@DefaultValue("86400s") Duration defaultTtl, @DefaultValue("604800s") Duration max) {}

  public record Idempotency(@DefaultValue("1h") Duration ttl, @DefaultValue("30s") Duration pendingTtl,
      @DefaultValue("2s") Duration waitFor) {}

  public record SeqEpochs(@DefaultValue("5s") Duration refresh) {}

  public record Grpc(@DefaultValue("9443") int port, @DefaultValue("false") boolean plaintext,
      @DefaultValue("1000") int maxInFlightPerStream,
      @DefaultValue({"spiffe://pangate/cell-1/gateway", "spiffe://pangate/cell-1/push-router"})
      List<String> allowedSpiffe,
      @DefaultValue Tls tls) {}

  public record Tls(@DefaultValue("push-api.crt") Path cert, @DefaultValue("push-api.key") Path key,
      @DefaultValue("spiffe-bundle.pem") Path trust) {}

  public record Http(@DefaultValue("128KB") DataSize maxBody, @DefaultValue("100") int maxBatch,
      @DefaultValue("1MB") DataSize maxBatchBody) {}
}
