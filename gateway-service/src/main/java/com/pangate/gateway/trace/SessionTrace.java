package com.pangate.gateway.trace;

import com.pangate.contracts.v1.FrameType;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-session trace log for kill switch trace.principal (spec §7.8): logger {@code pangate.trace} at INFO, only for
 * sessions flagged {@link Session#traced()}. The principal appears hashed only (spec §8); payloads never.
 */
public final class SessionTrace {

  private static final Logger log = LoggerFactory.getLogger("pangate.trace");
  private static volatile PrincipalHasher hasher = PrincipalHasher.random();

  public static final int MAX_LINES_PER_SEC = 1000;

  private SessionTrace() {}

  private static volatile java.util.function.LongSupplier nanos = System::nanoTime;
  private static final java.util.concurrent.atomic.AtomicLong windowStart =
      new java.util.concurrent.atomic.AtomicLong(Long.MIN_VALUE);
  private static final java.util.concurrent.atomic.AtomicInteger inWindow =
      new java.util.concurrent.atomic.AtomicInteger();

  static void clock(java.util.function.LongSupplier n) {
    nanos = n;
    windowStart.set(Long.MIN_VALUE);
    inWindow.set(0);
  }

  /** At most {@link #MAX_LINES_PER_SEC} lines per second per node (spec §8 caps lifecycle logs the same way). */
  private static boolean admit() {
    long now = nanos.getAsLong();
    long start = windowStart.get();
    if ((start == Long.MIN_VALUE || now - start >= 1_000_000_000L) && windowStart.compareAndSet(start, now)) {
      inWindow.set(0);  // lock-free: runs on gw-io; a few extra lines at a window edge are fine
    }
    return inWindow.incrementAndGet() <= MAX_LINES_PER_SEC;
  }

  /** Installs the cell's hash key at start-up, so hashes match across nodes and restarts. */
  public static void install(PrincipalHasher h) {
    hasher = h;
  }

  public static String principalHash(Session s) {
    return s.identity() == null ? "-" : hasher.hash(s.identity().principalId());
  }

  public static String hash(String principal) {
    return principal == null || principal.isEmpty() ? "-" : hasher.hash(principal);
  }

  public static void activated(Session s) {
    if (!admit()) {
      return;
    }
    log.info("trace p={} conn={} activated tenant={} device_platform={}", principalHash(s), s.connId(),
        s.identity().tenantId(), s.identity().platform());
  }

  public static void frame(Session s, String dir, FrameType t, String msgId, int bytes) {
    if (!admit()) {
      return;
    }
    log.info("trace p={} conn={} {} {} msg_id={} bytes={}", principalHash(s), s.connId(), dir,
        t.name().substring("FRAME_TYPE_".length()), msgId.isEmpty() ? "-" : msgId, bytes);
  }

  public static void closed(Session s, CloseCode c, String reason) {
    if (!admit()) {
      return;
    }
    log.info("trace p={} conn={} closed code={} reason={}", principalHash(s), s.connId(), c.code(), reason);
  }
}
