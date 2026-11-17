package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.UserRecord;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Users account-service told chat-service about (account spec §8), read through a small cache: a DM send does not
 * read ScyllaDB each time. {@link #ANY} knows everybody (tests and the constructors kept from CH1).
 */
public final class UserDirectory {

  public static final UserDirectory ANY = new UserDirectory(null);
  /** An active user is re-read after this; deleted is final and never re-read; unknown is never cached. */
  static final long ACTIVE_CACHE_MS = 10_000;
  static final int MAX_CACHED = 100_000;

  private record Cached(UserRecord record, long untilMs) {}

  private final ChatStores.UserStore users;
  private final LongSupplier nowMs;
  private final Map<String, Cached> cache = new ConcurrentHashMap<>();

  public UserDirectory(ChatStores.UserStore users) {
    this(users, System::currentTimeMillis);
  }

  public UserDirectory(ChatStores.UserStore users, LongSupplier nowMs) {
    this.users = users;
    this.nowMs = nowMs;
  }

  private Optional<UserRecord> lookup(String userId) {
    long now = nowMs.getAsLong();
    Cached c = cache.get(userId);
    if (c != null && c.untilMs() > now) {
      return Optional.of(c.record());
    }
    Optional<UserRecord> r = users.user(userId);
    if (r.isPresent()) {
      if (cache.size() >= MAX_CACHED) {
        cache.clear();
      }
      cache.put(userId, new Cached(r.get(), "deleted".equals(r.get().status()) ? Long.MAX_VALUE
          : now + ACTIVE_CACHE_MS));
    } else {
      cache.remove(userId);
    }
    return r;
  }

  /** Drops the cached state of a user whose account.users record this pod just applied. */
  public void forget(String userId) {
    cache.remove(userId);
  }

  /** Refuses an unknown ({@code user_not_found}) or deleted ({@code user_deleted}) user. */
  public void requireActive(String userId) {
    if (users == null) {
      return;
    }
    Optional<UserRecord> r = lookup(userId);
    if (r.isEmpty()) {
      throw new ChatRefusal(ChatRefusal.USER_NOT_FOUND, "no such user");
    }
    if ("deleted".equals(r.get().status())) {
      throw new ChatRefusal(ChatRefusal.USER_DELETED, "this user deleted the account");
    }
  }

  public boolean isDeleted(String userId) {
    return users != null && lookup(userId).map(r -> "deleted".equals(r.status())).orElse(false);
  }
}
