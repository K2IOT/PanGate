package com.pangate.chat.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory {@link PresenceState} with the same set semantics as {@code RedisPresence}. */
public final class FakePresence implements PresenceState {

  public final Map<String, Set<String>> conns = new ConcurrentHashMap<>();
  public final Map<String, Set<String>> byNode = new ConcurrentHashMap<>();
  public final Map<String, Long> gone = new ConcurrentHashMap<>();
  public final Map<String, Long> lastSeen = new ConcurrentHashMap<>();
  public final Set<String> events = ConcurrentHashMap.newKeySet();

  @Override public synchronized boolean add(String userId, String node, long epoch, String connId) {
    Set<String> s = conns.computeIfAbsent(userId, k -> new HashSet<>());
    boolean wasEmpty = s.isEmpty();
    s.add(node + "/" + connId);
    byNode.computeIfAbsent(node + ":" + epoch, k -> new HashSet<>()).add(userId + "/" + connId);
    return wasEmpty;
  }

  @Override public synchronized boolean remove(String userId, String node, long epoch, String connId) {
    Set<String> s = conns.getOrDefault(userId, new HashSet<>());
    boolean removed = s.remove(node + "/" + connId);
    byNode.getOrDefault(node + ":" + epoch, new HashSet<>()).remove(userId + "/" + connId);
    return removed && s.isEmpty();
  }

  @Override public synchronized List<String> dropNode(String node, long epoch) {
    List<String> emptied = new ArrayList<>();
    for (String uc : byNode.getOrDefault(node + ":" + epoch, Set.of())) {
      int slash = uc.indexOf('/');
      String user = uc.substring(0, slash);
      Set<String> s = conns.getOrDefault(user, new HashSet<>());
      if (s.remove(node + "/" + uc.substring(slash + 1)) && s.isEmpty()) {
        emptied.add(user);
      }
    }
    byNode.remove(node + ":" + epoch);
    return emptied;
  }

  @Override public boolean online(String userId) {
    return !conns.getOrDefault(userId, Set.of()).isEmpty();
  }

  @Override public boolean seen(String eventId) {
    return !events.add(eventId);
  }

  @Override public void markGone(String userId, long atMs) {
    gone.put(userId, atMs);
  }

  @Override public OptionalLong gone(String userId) {
    Long v = gone.get(userId);
    return v == null ? OptionalLong.empty() : OptionalLong.of(v);
  }

  @Override public void clearGone(String userId) {
    gone.remove(userId);
  }

  @Override public OptionalLong lastSeen(String userId) {
    Long v = lastSeen.get(userId);
    return v == null ? OptionalLong.empty() : OptionalLong.of(v);
  }

  @Override public void putLastSeen(String userId, long atMs) {
    lastSeen.put(userId, atMs);
  }
}
