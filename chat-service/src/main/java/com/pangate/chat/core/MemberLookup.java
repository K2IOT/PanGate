package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Member;
import java.util.List;
import java.util.Optional;

/**
 * Members of a conversation through the cache (Redis, TTL 60 s); a miss reads the store and fills the cache. Perf 3a:
 * the list is for DM and groups (at most 500) only, never a channel; one member is checked with {@link #member}.
 */
public final class MemberLookup {

  private final ChatStores.MemberStore store;
  private final ChatStores.MemberCache cache;

  public MemberLookup(ChatStores.MemberStore store, ChatStores.MemberCache cache) {
    this.store = store;
    this.cache = cache;
  }

  public List<Member> of(String convId) {
    Optional<List<Member>> hit = cache.cachedMembers(convId);
    if (hit.isPresent()) {
      return hit.get();
    }
    List<Member> ms = store.members(convId);
    cache.putMembers(convId, ms);
    return ms;
  }

  /**
   * Perf 3a (spec §3.1): whether one user is a member, read from the store row every time (no cache), so a removed
   * member loses access at once. The only way to check membership; {@link #of} is for DM and group lists.
   */
  public Optional<Member> member(String convId, String userId) {
    return store.member(convId, userId);
  }

  /** After a membership change: the next read goes to the store. False when that is not certain. */
  public boolean evict(String convId) {
    return cache.evictMembers(convId);
  }
}
