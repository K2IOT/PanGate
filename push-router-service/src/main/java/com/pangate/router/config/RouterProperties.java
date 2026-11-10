package com.pangate.router.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** {@code router.*}, defaults of push-router spec §5. */
@ConfigurationProperties("router")
public record RouterProperties(
    @DefaultValue("c1") String cell,
    @DefaultValue Kafka kafka,
    @DefaultValue("6s") Duration ackTimeout,
    @DefaultValue({"100ms", "400ms"}) List<Duration> retries,
    @DefaultValue Registry registry,
    @DefaultValue("1s") Duration directoryRefresh,
    @DefaultValue Batch batch,
    @DefaultValue("10000") int maxInFlightPerNode,
    @DefaultValue Grpc grpc) {

  public record Kafka(@DefaultValue("kafka:9092") String bootstrap, @DefaultValue("push-router") String group,
      @DefaultValue("20000") int maxConcurrency) {}

  public record Registry(@DefaultValue("redis://redis-registry:6379") String uri,
      @DefaultValue("cluster") String topology, @DefaultValue("2s") Duration cache,
      @DefaultValue("200ms") Duration timeout) {}

  public record Batch(@DefaultValue("256") int max, @DefaultValue("2ms") Duration linger) {}

  public record Grpc(@DefaultValue("false") boolean plaintext, @DefaultValue Tls tls) {}

  public record Tls(@DefaultValue("router.crt") Path cert, @DefaultValue("router.key") Path key,
      @DefaultValue("spiffe-bundle.pem") Path trust) {}
}
