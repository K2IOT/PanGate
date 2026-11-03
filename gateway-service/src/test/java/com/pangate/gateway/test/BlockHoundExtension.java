package com.pangate.gateway.test;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import reactor.blockhound.BlockHound;

/** Installs BlockHound once per test JVM: gateway event loop threads (gw-io-*) must never block. */
public final class BlockHoundExtension implements BeforeAllCallback {

  private static volatile boolean installed;

  @Override
  public void beforeAll(ExtensionContext context) {
    install();
  }

  public static synchronized void install() {
    if (!installed) {
      BlockHound.builder()
          .nonBlockingThreadPredicate(p -> p.or(t -> t.getName().startsWith("gw-io-")))
          .install();
      installed = true;
    }
  }
}
