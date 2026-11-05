package com.pangate.gateway.lifecycle;

/**
 * Terminates the process (fence, control catch-up timeout). Production logs and calls {@code System.exit} on a fresh
 * thread so a library I/O thread (Lettuce, Kafka) never runs the shutdown hooks itself; tests record the call.
 */
@FunctionalInterface
public interface ProcessExit {

  void exit(int status, String reason);

  static ProcessExit system() {
    return (status, reason) -> {
      org.slf4j.LoggerFactory.getLogger(ProcessExit.class).error("exiting with status {}: {}", status, reason);
      Thread t = new Thread(() -> System.exit(status), "gw-exit");
      t.setDaemon(false);
      t.start();
    };
  }
}
