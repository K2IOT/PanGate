package com.pangate.chat.core;

import java.util.List;
import java.util.OptionalLong;

/**
 * Connections per user, shared by every pod (CH3 spec §6.1; Redis). {@code add} / {@code remove} answer whether the
 * user's set changed between empty and not empty. Every method may throw {@link StoreUnavailableException}.
 */
public interface PresenceState {

  /** True when the user had no connection before. */
  boolean add(String userId, String node, long epoch, String connId);

  /** True when this removed the user's last connection. */
  boolean remove(String userId, String node, long epoch, String connId);

  /** Drops every connection of a node; returns the users left with none. */
  List<String> dropNode(String node, long epoch);

  boolean online(String userId);

  /** True when this event id was handled before (dedup, kept 1 h); marks it handled otherwise. */
  boolean seen(String eventId);

  /** The user's last connection closed at {@code atMs}; offline once the grace ends. */
  void markGone(String userId, long atMs);

  OptionalLong gone(String userId);

  void clearGone(String userId);

  OptionalLong lastSeen(String userId);

  void putLastSeen(String userId, long atMs);
}
