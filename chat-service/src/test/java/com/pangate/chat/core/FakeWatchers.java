package com.pangate.chat.core;

import com.pangate.contracts.v1.PresenceWatch;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/** In-memory {@link Watchers}: device → (entries, expiry). */
public final class FakeWatchers implements Watchers {

  record Watch(List<PresenceWatch.Entry> entries, Set<String> open, long until) {}

  public final Map<String, Watch> byDevice = new ConcurrentHashMap<>();
  private final LongSupplier wallMs;

  public FakeWatchers(LongSupplier wallMs) {
    this.wallMs = wallMs;
  }

  @Override public void replace(String viewer, String device, List<PresenceWatch.Entry> valid, Set<String> open,
      long expiresAtMs) {
    byDevice.put(viewer + "/" + device, new Watch(List.copyOf(valid), Set.copyOf(open), expiresAtMs));
  }

  @Override public List<String> watchersOf(String userId, long nowMs) {
    return byDevice.entrySet().stream().filter(e -> e.getValue().until() > nowMs)
        .flatMap(e -> e.getValue().entries().stream().filter(x -> x.getUserId().equals(userId))
            .map(x -> e.getKey() + "/" + x.getConversationId()))
        .toList();
  }

  @Override public Set<String> devicesWithOpen(String convId) {
    long now = wallMs.getAsLong();
    return byDevice.entrySet().stream().filter(e -> e.getValue().until() > now)
        .filter(e -> e.getValue().open().contains(convId))
        .map(Map.Entry::getKey).collect(Collectors.toSet());
  }
}
