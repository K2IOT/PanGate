package com.pangate.chat.core;

import com.pangate.contracts.testkit.ChatTopics;
import java.util.Optional;

/** Principal {@code chat:<userId>} ↔ user id (chat spec §1). */
public final class UserIds {

  private UserIds() {}

  /** The user id of a {@code chat:} principal; empty for any other principal or an empty user id. */
  public static Optional<String> fromPrincipal(String principal) {
    if (principal == null || !principal.startsWith(ChatTopics.PRINCIPAL_PREFIX)
        || principal.length() == ChatTopics.PRINCIPAL_PREFIX.length()) {
      return Optional.empty();
    }
    return Optional.of(principal.substring(ChatTopics.PRINCIPAL_PREFIX.length()));
  }

  public static String principal(String userId) {
    return ChatTopics.PRINCIPAL_PREFIX + userId;
  }
}
