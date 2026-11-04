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
    Kafka kafka,
    Grpc grpc,
    PushApi pushApi,
    Broadcast broadcast,
    DataSize outboundBudget,
    Recycling recycling,
    Drain drain,
    Warmup warmup,
    Shed shed) {

  public record Listeners(Ws ws, Tcp tcp) {}

  public record Ws(int port, String path, int acceptors) {}

  public record Tcp(int port, boolean enabled) {}

  public record Io(int threads, Transport transport) {}

  /** {@code reloadInterval}: how often cert and key files are checked for replacement (spec §7.7). */
  public record Tls(Path certFile, Path keyFile, java.time.Duration reloadInterval) {
    public Tls {
      reloadInterval = reloadInterval == null ? java.time.Duration.ofSeconds(30) : reloadInterval;
    }
  }

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
      int topicsPerConn,
      PendingAck pendingAck,
      Duration slowConsumerTimeout) {}

  /** Connection recycling (spec §5.9): RECONNECT after U[min, max], closed after delay + grace. */
  public record Recycling(Duration min, Duration max, Duration grace) {}

  /** Staged drain (spec §7.5). */
  public record Drain(int batchPercent, Duration interval, Duration maxDelay, Duration maxDuration,
      Duration listenerCloseDelay) {}

  /**
   * Shed ladder inputs (spec §7.3). {@code cpu=false} drops the CPU signals (L1, L3): local and dev machines share
   * their CPU with the IDE, tests and JIT, which would otherwise refuse new connections for a minute at a time.
   */
  public record Shed(boolean cpu) {}

  /** Warm-up before readiness (spec §7.6). */
  public record Warmup(int iterations, int concurrency, Duration timeout) {}

  /** Downstream pending-ack window per session (spec §5.3, §5.4). */
  public record PendingAck(int msgs, DataSize bytes, Duration timeout) {}

  /** PushStream server for the push router (spec §6.3); plaintext only in local and dev. */
  public record Grpc(int port, boolean plaintext, List<String> allowedSpiffe, GrpcTls tls) {}

  public record GrpcTls(Path certFile, Path keyFile, Path trustFile) {}

  /** Push API target for single-session KICK (spec §6.6). */
  public record PushApi(boolean enabled, String target, boolean plaintext) {}

  public record Broadcast(int ratePerLoop) {}

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
