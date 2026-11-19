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
    return total(userId, null, 0);
  }

  /**
   * Review 2026-11-19 (F4): the badge of a notification for message {@code convSeq} of {@code convId}. Fan-out raises
   * the inbox rows only after its pushes, so the row may not hold this message yet: it counts as at least that seq
   * (and alone when the row is not there).
   */
  public int total(String userId, String convId, long convSeq) {
    long sum = 0;
    boolean seen = false;
    for (ChatStores.UserConv u : inbox.userConvs(userId, null, MAX_CONVERSATIONS)) {
      long last = u.lastSeq();
      if (u.convId().equals(convId)) {
        seen = true;
        last = Math.max(last, convSeq);
      }
      sum += Math.max(0, last - u.readSeq());
    }
    if (!seen && convId != null && convSeq > 0) {
      sum += 1;
    }
    return (int) Math.min(Integer.MAX_VALUE, sum);
  }
}
