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
      builder.nonBlockingThreadPredicate(p -> p.or(t -> t.getName().startsWith("gw-io-"))).install();
      installed = true;
    }
  }
}
