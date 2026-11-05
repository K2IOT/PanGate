package com.pangate.gateway.downstream;

import com.pangate.gateway.session.Session;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.List;

/**
 * Topic → subscribed sessions of one event loop (spec §6.4); {@code session.topics} mirrors it for removal on close.
 * Owner loop only.
 */
public final class TopicIndex {

  private final Object2ObjectOpenHashMap<String, ObjectArrayList<Session>> topics = new Object2ObjectOpenHashMap<>();

  public boolean add(Session s, String topic) {
    if (!s.addTopic(topic)) {
      return false;
    }
    topics.computeIfAbsent(topic, k -> new ObjectArrayList<>(2)).add(s);
    return true;
  }

  public boolean remove(Session s, String topic) {
    if (!s.removeTopic(topic)) {
      return false;
    }
    unlink(s, topic);
    return true;
  }

  public void removeAll(Session s) {
    String[] held = s.topics();
    if (held == null) {
      return;
    }
    for (String t : held) {
      s.removeTopic(t);
      unlink(s, t);
    }
  }

  private void unlink(Session s, String topic) {
    ObjectArrayList<Session> members = topics.get(topic);
    if (members != null) {
      members.remove(s);
      if (members.isEmpty()) {
        topics.remove(topic);
      }
    }
  }

  /** A copy, so the caller may keep iterating while sessions come and go. */
  public List<Session> snapshot(String topic) {
    ObjectArrayList<Session> members = topics.get(topic);
    return members == null ? List.of() : new ObjectArrayList<>(members);
  }

  public int topicCount() {
    return topics.size();
  }
}
