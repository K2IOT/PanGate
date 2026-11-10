package com.pangate.contracts.testkit;

import java.time.Duration;
import java.util.List;

/** Push-path topics of one cell (push contracts spec §2). */
public final class PushTopics {

  /** A topic as the spec sizes it. */
  public record Spec(String name, int partitions, Duration retention) {}

  private PushTopics() {}

  public static Spec requests(String cell) {
    return new Spec(cell + ".push.requests", 64, Duration.ofHours(24));
  }

  public static Spec offline(String cell) {
    return new Spec(cell + ".push.offline", 16, Duration.ofDays(7));
  }

  public static Spec delivered(String cell) {
    return new Spec(cell + ".push.delivered", 16, Duration.ofHours(24));
  }

  public static Spec lifecycle(String cell) {
    return new Spec(cell + ".gw.lifecycle", 32, Duration.ofHours(24));
  }

  public static List<Spec> all(String cell) {
    return List.of(requests(cell), offline(cell), delivered(cell), lifecycle(cell));
  }
}
