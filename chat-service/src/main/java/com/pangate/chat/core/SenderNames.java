package com.pangate.chat.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** CH4 spec §3.2: a sender's display name, cached 60 s per pod; the user id when unknown. */
public final class SenderNames {

  static final long TTL_MS = 60_000;
  static final int MAX = 100_000;

  private record Name(String value, long untilMs) {}

  private final ChatStores.UserStore users;
  private final Map<String, Name> names = new ConcurrentHashMap<>();

  public SenderNames(ChatStores.UserStore users) {
    this.users = users;
  }

  public String of(String userId, long nowMs) {
    Name n = names.get(userId);
    if (n != null && n.untilMs() > nowMs) {
      return n.value();
    }
    String v = users.displayName(userId).orElse(userId);
    if (names.size() > MAX) {
      names.clear();
    }
    names.put(userId, new Name(v, nowMs + TTL_MS));
    return v;
  }
}
