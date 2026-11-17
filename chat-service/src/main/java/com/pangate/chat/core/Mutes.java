package com.pangate.chat.core;

import java.util.List;

/** CH4 spec §3.3: a conversation muted until a time (0 = not, {@link Long#MAX_VALUE} = for ever). */
public final class Mutes {

  private Mutes() {}

  public static boolean muted(long mutedUntil, long nowMs) {
    return mutedUntil > nowMs;
  }

  /** Muted conversations stay silent, except for a message that mentions the user (as Telegram, spec §9.1). */
  public static boolean shouldNotify(long mutedUntil, List<String> mentions, String userId, long nowMs) {
    return !muted(mutedUntil, nowMs) || mentions.contains(userId);
  }
}
