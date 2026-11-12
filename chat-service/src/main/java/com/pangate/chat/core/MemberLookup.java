package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Member;
import java.util.List;
import java.util.Optional;

/** Members of a conversation through the cache (Redis, TTL 60 s); a miss reads the store and fills the cache. */
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

  /** After a membership change: the next read goes to the store. False when that is not certain. */
  public boolean evict(String convId) {
    return cache.evictMembers(convId);
  }
}
