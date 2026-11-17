package com.pangate.chat.core;

import java.util.List;

/**
 * Who is watching whom (CH3 spec §6.3; Redis). An entry lives until {@code expiresAtMs}; a device's new list replaces
 * its old one. Also tells typing which devices have a conversation open. May throw {@link StoreUnavailableException}.
 */
public interface Watchers extends OpenConversations {

  /**
   * Replaces the device's list: {@code valid} are the presence entries kept; {@code open} the conversations it has
   * open (typing, §5), kept even when no entry in them may show presence (Codex review of PR #6).
   */
  void replace(String viewer, String device, List<com.pangate.contracts.v1.PresenceWatch.Entry> valid,
      java.util.Set<String> open, long expiresAtMs);

  default void replace(String viewer, String device, List<com.pangate.contracts.v1.PresenceWatch.Entry> valid,
      long expiresAtMs) {
    java.util.Set<String> open = new java.util.LinkedHashSet<>();
    valid.forEach(e -> open.add(e.getConversationId()));
    replace(viewer, device, valid, open, expiresAtMs);
  }

  /** {@code <viewer>/<device>/<conversationId>} of the live entries watching {@code userId}. */
  List<String> watchersOf(String userId, long nowMs);
}
