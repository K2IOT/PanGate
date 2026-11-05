package com.pangate.gateway.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.dev.DevSecretsGenerator;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.pipeline.SslContextProvider;
import com.pangate.gateway.pipeline.TlsTicketKeys;
import com.pangate.starter.secrets.FileSecretSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EdgeConfigTest {

  @Test
  void ticketKeyRotationWaitsForAnInFlightCertificateReload(@TempDir Path d) throws Exception {
    DevSecretsGenerator.ensure(d);
    GatewayMetrics m = new GatewayMetrics(new SimpleMeterRegistry());
    SslContextProvider tls = new SslContextProvider(new GatewayProperties.Tls(d.resolve("gateway.crt"),
        d.resolve("gateway.key"), Duration.ofSeconds(30)), Executors.newSingleThreadScheduledExecutor(),
        Duration.ZERO, m);
    FileSecretSource secrets = new FileSecretSource(d);
    AtomicReference<TlsTicketKeys> keys = new AtomicReference<>(TlsTicketKeys.load(secrets));
    java.nio.file.Files.delete(d.resolve("tls-ticket-keys/current"));
    DevSecretsGenerator.ensure(d);  // rotated current key

    Thread rotation;
    synchronized (tls) {  // what reloadIfChanged holds while it swaps contexts
      rotation = Thread.ofPlatform().start(() -> EdgeConfig.reloadTlsTicketKeys(secrets, tls, keys, m));
      Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> rotation.getState() == Thread.State.BLOCKED);
      assertThat(keys.get()).isNotEqualTo(TlsTicketKeys.load(secrets));
    }
    rotation.join(2000);
    assertThat(keys.get()).isEqualTo(TlsTicketKeys.load(secrets));
    tls.close();
  }

  @Test
  void ticketKeyRotationAfterShutdownIsSkippedQuietly(@TempDir Path d) throws Exception {
    DevSecretsGenerator.ensure(d);
    SimpleMeterRegistry meters = new SimpleMeterRegistry();
    GatewayMetrics m = new GatewayMetrics(meters);
    SslContextProvider tls = new SslContextProvider(new GatewayProperties.Tls(d.resolve("gateway.crt"),
        d.resolve("gateway.key"), Duration.ofSeconds(30)), Executors.newSingleThreadScheduledExecutor(),
        Duration.ZERO, m);
    FileSecretSource secrets = new FileSecretSource(d);
    AtomicReference<TlsTicketKeys> keys = new AtomicReference<>(TlsTicketKeys.load(secrets));
    java.nio.file.Files.delete(d.resolve("tls-ticket-keys/current"));
    DevSecretsGenerator.ensure(d);  // rotated current key
    tls.close();

    EdgeConfig.reloadTlsTicketKeys(secrets, tls, keys, m);

    assertThat(meters.find("gw.secrets.reload").tag("result", "failed").counter()).isNull();
  }
}
