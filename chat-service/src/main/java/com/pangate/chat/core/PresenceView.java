package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Privacy;
import com.pangate.contracts.v1.PresenceUpdate;
import java.util.OptionalLong;

/** CH3 spec §6.2, §8: what a viewer is told about one user. */
public final class PresenceView {

  static final long MINUTE_MS = 60_000;
  static final long RECENTLY_AFTER_MS = 7L * 24 * 3_600_000;

  private PresenceView() {}

  public static PresenceUpdate.State state(String userId, boolean online, OptionalLong lastSeenMs, Privacy subject,
      Privacy viewer, long nowMs) {
    PresenceUpdate.State.Builder b = PresenceUpdate.State.newBuilder().setUserId(userId);
    if (!PrivacyRules.mayShowPresence(subject, viewer)) {
      return b.setHidden(true).build();
    }
    if (online) {
      return b.setOnline(true).build();
    }
    if (lastSeenMs.isPresent()) {
      long at = lastSeenMs.getAsLong();
      if (nowMs - at > RECENTLY_AFTER_MS) {
        b.setRecently(true);
      } else {
        b.setLastSeenMs(at / MINUTE_MS * MINUTE_MS);
      }
    }
    return b.build();
  }
}
