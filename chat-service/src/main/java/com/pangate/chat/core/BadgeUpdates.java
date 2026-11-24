package com.pangate.chat.core;

import java.util.List;

/**
 * Perf 3a (spec §3.3): what keeps the badge counters in step, called after the matching store write. Never throws: the
 * badge is secondary, so a failure never breaks a fan-out, a receipt or a membership change.
 */
public interface BadgeUpdates {

  /** A conversation's last / read seq for this user moved (0 = unchanged). */
  void raise(String user, String convId, long last, long read);

  /** A new message {@code last} for each of {@code users} (the sender is raised on their own). */
  void raiseAll(List<String> users, String convId, long last);

  /** The user joined at {@code last} (perf 3a final review I1: also after leaving it before). */
  void join(String user, String convId, long last);

  /** The user left the conversation. */
  void drop(String user, String convId);

  BadgeUpdates NONE = new BadgeUpdates() {
    @Override
    public void raise(String user, String convId, long last, long read) {}

    @Override
    public void raiseAll(List<String> users, String convId, long last) {}

    @Override
    public void join(String user, String convId, long last) {}

    @Override
    public void drop(String user, String convId) {}
  };
}
