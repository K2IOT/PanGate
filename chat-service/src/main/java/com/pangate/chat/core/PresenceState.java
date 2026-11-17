package com.pangate.chat.core;

import java.util.List;
import java.util.OptionalLong;

/**
 * Connections per user, shared by every pod (CH3 spec §6.1; Redis). {@code add} / {@code remove} answer whether the
 * user's set changed between empty and not empty. Every method may throw {@link StoreUnavailableException}.
 */
public interface PresenceState {

  /**
   * True when the user had no connection before. A connection of a node already reported down (NODE_DOWN tombstone,
   * 24 h) is ignored: a late CONNECTED must not resurrect it (final review I7).
   */
  boolean add(String userId, String node, long epoch, String connId);

  /** True when this removed the user's last connection. */
  boolean remove(String userId, String node, long epoch, String connId);

  /** Drops every connection of a node and remembers it as down; returns the users left with none. */
  List<String> dropNode(String node, long epoch);

  boolean online(String userId);

  /** The online users of a page (CH4 spec §4.2: a channel's followers online now get no notification). */
  default java.util.Set<String> onlineOf(java.util.Collection<String> userIds) {
    java.util.Set<String> out = new java.util.HashSet<>();
    for (String u : userIds) {
      if (online(u)) {
        out.add(u);
      }
    }
    return out;
  }

  /** True when this event id was handled before (dedup, kept 1 h); marks it handled otherwise. */
  boolean seen(String eventId);

  /** The user's last connection closed at {@code atMs}; offline once the grace ends. */
  void markGone(String userId, long atMs);

  OptionalLong gone(String userId);

  void clearGone(String userId);

  OptionalLong lastSeen(String userId);

  void putLastSeen(String userId, long atMs);

  /** The offline graces, shared by every pod (final review I6): due at {@code dueAtMs}. */
  void scheduleGrace(String userId, long dueAtMs);

  /** Users whose grace is due. */
  List<String> dueGraces(long nowMs);

  /** Takes a due grace; true for exactly one caller. */
  boolean claimGrace(String userId);

  void cancelGrace(String userId);
}
