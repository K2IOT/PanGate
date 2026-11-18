package com.pangate.chat.core;

/**
 * CH4 spec §3.1: the app icon's number, every unread message of the conversations not left (muted ones too). Capped at
 * the first {@link #MAX_CONVERSATIONS} conversations to bound the read per notification (review of PR #7: kept on
 * purpose; a user in more gets a lower number).
 */
public final class Badge {

  static final int MAX_CONVERSATIONS = 10_000;

  private final ChatStores.MemberStore inbox;

  public Badge(ChatStores.MemberStore inbox) {
    this.inbox = inbox;
  }

  public int total(String userId) {
    long sum = 0;
    for (ChatStores.UserConv u : inbox.userConvs(userId, null, MAX_CONVERSATIONS)) {
      sum += Math.max(0, u.lastSeq() - u.readSeq());
    }
    return (int) Math.min(Integer.MAX_VALUE, sum);
  }
}
