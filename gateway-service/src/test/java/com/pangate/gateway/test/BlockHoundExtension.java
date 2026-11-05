package com.pangate.gateway.test;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import java.util.ServiceLoader;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

/** Installs BlockHound once per test JVM: gateway event loop threads (gw-io-*) must never block. */
public final class BlockHoundExtension implements BeforeAllCallback {

  private static volatile boolean installed;

  @Override
  public void beforeAll(ExtensionContext context) {
    install();
  }

  public static synchronized void install() {
    if (!installed) {
      BlockHound.Builder builder = BlockHound.builder();
      // Library integrations (e.g. Netty's) whitelist their known-safe internal blocking points.
      ServiceLoader.load(BlockHoundIntegration.class).forEach(builder::with);
      // max.block.ms=0: send() never waits for metadata or buffer space; it may only take the accumulator's
      // short-held locks, which BlockHound would report as parking under contention.
      builder.allowBlockingCallsInside("org.apache.kafka.clients.producer.KafkaProducer", "send");
      // Lettuce's command dispatch takes SharedLock (a ReentrantLock held only to enqueue the command); under
      // contention it parks for microseconds. Caught by GatewayProtectionIT (HDEL on close under load).
      builder.allowBlockingCallsInside("io.lettuce.core.protocol.SharedLock", "incrementWriters");
      builder.allowBlockingCallsInside("io.lettuce.core.protocol.SharedLock", "decrementWriters");
      // Netty's integration marks every Netty thread non-blocking, Lettuce's own event loops ("lettuce-*") included.
      // Those are not ours to police: on close Lettuce takes SharedLock exclusively there and may park briefly when
      // another thread closes the same connection (flaked TenantCapBorrowerIT teardown once in M3b; see
      // io.lettuce.core.protocol.SharedLockBlockHoundTest). gw-io-* threads stay strict, for that lock too.
      builder.nonBlockingThreadPredicate(p -> p.and(t -> !t.getName().startsWith("lettuce-"))
          .or(t -> t.getName().startsWith("gw-io-"))).install();
      installed = true;
    }
  }
}
