package com.pangate.chat.core;

/**
 * Perf 3b (spec §4.1): chat dropped a BEST_EFFORT or DROPPABLE push because the Push API had no room for it. Final:
 * not a failure to retry (unlike {@link PushUnavailableException}).
 */
public final class PushDroppedException extends RuntimeException {

  public static final String WINDOW_FULL = "window_full";
  public static final String WAITED = "waited";
  public static final String QUEUE_FULL = "queue_full";

  private final String reason;

  public PushDroppedException(String reason) {
    super("push dropped: " + reason, null, false, false);
    this.reason = reason;
  }

  public String reason() {
    return reason;
  }
}
