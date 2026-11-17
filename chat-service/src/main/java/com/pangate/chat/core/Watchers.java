package com.pangate.chat.core;

import java.util.List;

/**
 * Who is watching whom (CH3 spec §6.3; Redis). An entry lives until {@code expiresAtMs}; a device's new list replaces
 * its old one. Also tells typing which devices have a conversation open. May throw {@link StoreUnavailableException}.
 */
public interface Watchers extends OpenConversations {

  void replace(String viewer, String device, List<com.pangate.contracts.v1.PresenceWatch.Entry> valid,
      long expiresAtMs);

  /** {@code <viewer>/<device>/<conversationId>} of the live entries watching {@code userId}. */
  List<String> watchersOf(String userId, long nowMs);
}
