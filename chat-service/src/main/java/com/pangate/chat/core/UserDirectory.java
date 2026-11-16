package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.UserRecord;
import java.util.Optional;

/**
 * Users account-service told chat-service about (account spec §8). {@link #ANY} knows everybody (tests and the
 * constructors kept from CH1).
 */
public final class UserDirectory {

  public static final UserDirectory ANY = new UserDirectory(null);

  private final ChatStores.UserStore users;

  public UserDirectory(ChatStores.UserStore users) {
    this.users = users;
  }

  /** Refuses an unknown ({@code user_not_found}) or deleted ({@code user_deleted}) user. */
  public void requireActive(String userId) {
    if (users == null) {
      return;
    }
    Optional<UserRecord> r = users.user(userId);
    if (r.isEmpty()) {
      throw new ChatRefusal(ChatRefusal.USER_NOT_FOUND, "no such user");
    }
    if ("deleted".equals(r.get().status())) {
      throw new ChatRefusal(ChatRefusal.USER_DELETED, "this user deleted the account");
    }
  }

  public boolean isDeleted(String userId) {
    return users != null && users.user(userId).map(r -> "deleted".equals(r.status())).orElse(false);
  }
}
