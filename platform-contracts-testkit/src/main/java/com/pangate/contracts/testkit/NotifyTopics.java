package com.pangate.contracts.testkit;

import java.time.Duration;

/** Notification topics of one cell (offline notifications spec §2.1). */
public final class NotifyTopics {

  private NotifyTopics() {}

  /** {@code <cell>.notify.requests}: 32 partitions, kept one day (an older request has expired). */
  public static PushTopics.Spec requests(String cell) {
    return new PushTopics.Spec(cell + ".notify.requests", 32, Duration.ofDays(1));
  }
}
