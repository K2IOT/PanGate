package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Privacy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/** A user's privacy settings, cached 60 s per pod (CH3 spec §8); the pod that changes them forgets at once. */
public final class PrivacyCache {

  static final long TTL_MS = 60_000;
  static final int MAX = 200_000;

  private record Entry(Privacy p, long until) {}

  private final ChatStores.SettingsStore store;
  private final LongSupplier monoMs;
  private final Map<String, Entry> byUser = new ConcurrentHashMap<>();

  public PrivacyCache(ChatStores.SettingsStore store, LongSupplier monoMs) {
    this.store = store;
    this.monoMs = monoMs;
  }

  public Privacy of(String userId) {
    long now = monoMs.getAsLong();
    Entry e = byUser.get(userId);
    if (e != null && now < e.until()) {
      return e.p();
    }
    if (byUser.size() >= MAX) {
      byUser.clear();
    }
    Privacy p = store.privacy(userId);
    byUser.put(userId, new Entry(p, now + TTL_MS));
    return p;
  }

  /**
   * Read from the store, never the cache (and refresh it). CH4 deferred minor: a DM or group preview is private,
   * so hiding previews on another pod must apply to the next notification, not up to 60 s later.
   */
  public Privacy fresh(String userId) {
    Privacy p = store.privacy(userId);
    byUser.put(userId, new Entry(p, monoMs.getAsLong() + TTL_MS));
    return p;
  }

  public void forget(String userId) {
    byUser.remove(userId);
  }
}
