package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Privacy;

/** CH3 spec §8: a state is shown only when neither side hides it (reciprocal). */
public final class PrivacyRules {

  private PrivacyRules() {}

  public static boolean mayShowPresence(Privacy subject, Privacy viewer) {
    return !subject.hidePresence() && !viewer.hidePresence();
  }

  public static boolean mayShowRead(Privacy subject, Privacy viewer) {
    return !subject.hideRead() && !viewer.hideRead();
  }
}
