package com.pangate.gateway.config;

import com.pangate.gateway.server.Transport;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.springframework.util.unit.DataSize;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Typed {@code gateway.*} configuration (spec §9). Defaults live in application.yml. */
@ConfigurationProperties("gateway")
public record GatewayProperties(
    String nodeId,
    String cell,
    Listeners listeners,
    Io io,
    Tls tls,
    Limits limits,
    Admission admission,
    Auth auth,
    Dev dev,
    Startup startup,
    Redis redis,
    Kafka kafka) {

  public record Listeners(Ws ws, Tcp tcp) {}

  public record Ws(int port, String path, int acceptors) {}

  public record Tcp(int port, boolean enabled) {}

  public record Io(int threads, Transport transport) {}

  public record Tls(Path certFile, Path keyFile) {}

  public record Limits(
      int hardCap,
      int softCap,
      int perIp,
      int frameMax,
      int helloMax,
      Duration helloTimeout,
      Duration handshakeTimeout,
      int handshakesPerPrincipalPerMin,
      Duration heartbeatInterval,
      Duration sessionTtl,
      int inboundRate,
      int inboundBurst,
      int topicsPerConn) {}

  public record Admission(int rate, int burst, int rampFrom, Duration rampDuration) {}

  public record Auth(
      String jwksUri, Path jwksFile, Duration jwksRefresh, String audience, String issuer, Duration clockSkew) {}

  public record Dev(boolean bootstrapSecrets, Path dir) {}

  public record Startup(Duration dependencyTimeout) {}

  /** Redis for the registry (lease, reg:{p}) and state (seq, seqepochs); one server may serve both (spec §9.1). */
  public record Redis(RedisTopology topology, String registryUri, String stateUri, Duration commandTimeout) {}

  public record Kafka(
      String bootstrap,
      boolean createTopics,
      List<String> upstreamTypes,
      String topicPrefix,
      DataSize reliableBuffer,
      DataSize telemetryBuffer) {}
}
