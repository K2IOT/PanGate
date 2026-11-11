package com.pangate.gateway.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.gateway.GatewayApplication;
import com.pangate.gateway.downstream.GrpcServerLifecycle;
import io.lettuce.core.RedisClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** The gateway publishes node:addr:{id} = host:bound gRPC port for push routers (push-router spec §3.1). */
class NodeAddressIT {

  @TempDir static Path devDir;

  @Test
  void addressIsTheBoundGrpcPortAndLivesWithTheLease() throws Exception {
    String nodeId = "gw-addr-" + UUID.randomUUID().toString().substring(0, 8);
    List<String> args = new ArrayList<>(List.of("--gateway.dev.dir=" + devDir,
        "--gateway.listeners.ws.port=" + GatewayClusterIT.freePort(),
        "--gateway.listeners.tcp.port=" + GatewayClusterIT.freePort(), "--server.port=" + GatewayClusterIT.freePort(),
        "--gateway.io.threads=2", "--gateway.grpc.port=0", "--gateway.warmup.iterations=0",
        "--gateway.node-id=" + nodeId));
    args.addAll(InfraContainers.gatewayArgs());
    RedisClient client = RedisClient.create(InfraContainers.redisUri());
    try (var redis = client.connect()) {
      var ctx = new SpringApplicationBuilder(GatewayApplication.class).profiles("local")
          .run(args.toArray(String[]::new));
      String key = "node:addr:{" + nodeId + "}";
      int port = ctx.getBean(GrpcServerLifecycle.class).port();
      try {
        await().atMost(Duration.ofSeconds(10)).until(() -> redis.sync().get(key) != null);
        assertThat(redis.sync().get(key)).isEqualTo("127.0.0.1:" + port);
        assertThat(redis.sync().ttl(key)).isBetween(1L, 15L);
        Thread.sleep(6_000);  // one renewal (every 5 s) later the TTL is fresh again
        assertThat(redis.sync().ttl(key)).isGreaterThan(9L);
      } finally {
        ctx.close();
      }
      assertThat(redis.sync().exists(key)).isZero();
    } finally {
      client.shutdown();
    }
  }
}
