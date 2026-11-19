package com.pangate.chat.core;

import com.pangate.contracts.v1.ConversationKind;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * CH4 spec §3.1: the app icon's number, every unread message of the DMs and groups not left (muted ones too). Perf 3a
 * (spec §3.3): read from per-user counters kept up to date by the fan-out, the receipt worker and membership changes
 * ({@link BadgeUpdates}), so a notification costs O(1) Redis calls. Counters missing or older than
 * {@link #REBUILD_AFTER_MS} are rebuilt from the first {@link #MAX_CONVERSATIONS} inbox rows (a user in more gets a
 * lower number); when Redis fails the inbox is summed as before.
 */
public final class Badge implements BadgeUpdates {

  static final int MAX_CONVERSATIONS = 10_000;
  /** Spec §3.3: a write lost while the counters survived is repaired within a day. */
  static final long REBUILD_AFTER_MS = 24L * 3_600_000L;
  /** Final review I2: after a Redis failure, how long the counters are left alone. */
  static final long PAUSE_MS = 5_000;
  static final int MAX_DIRTY = 100_000;

  private final ChatStores.MemberStore inbox;
  private final ChatStores.BadgeStore counters;
  private final LongSupplier wallMs;
  private final java.util.Set<String> dirty = java.util.concurrent.ConcurrentHashMap.newKeySet();
  private volatile long pausedUntil;

  /** Without counters: every badge sums the inbox. */
  public Badge(ChatStores.MemberStore inbox) {
    this(inbox, null, System::currentTimeMillis);
  }

  public Badge(ChatStores.MemberStore inbox, ChatStores.BadgeStore counters, LongSupplier wallMs) {
    this.inbox = inbox;
    this.counters = counters;
    this.wallMs = wallMs;
  }

  public int total(String userId) {
    return total(userId, null, 0);
  }

  /**
   * Review 2026-11-19 (F4): the badge of a notification for message {@code convSeq} of {@code convId}. Fan-out raises
   * the inbox rows only after its pushes, so the row may not hold this message yet: it counts as at least that seq.
   */
  public int total(String userId, String convId, long convSeq) {
    if (counters != null && usable()) {
      try {
        long now = wallMs.getAsLong();
        long t = counters.totalWith(userId, convId, convSeq, now - REBUILD_AFTER_MS);
        if (t < 0) {
          counters.rebuild(userId, snapshot(userId), now);
          t = counters.totalWith(userId, convId, convSeq, now - REBUILD_AFTER_MS);
        }
        if (t >= 0) {
          return (int) Math.min(Integer.MAX_VALUE, t);
        }
      } catch (RuntimeException e) {
        pause();   // the badge is secondary: sum the inbox
      }
    }
    return scan(userId, convId, convSeq);
  }

  private List<ChatStores.UserConv> rows(String userId) {
    List<ChatStores.UserConv> out = new ArrayList<>();
    for (ChatStores.UserConv u : inbox.userConvs(userId, null, MAX_CONVERSATIONS)) {
      if (u.kind() != ConversationKind.CONVERSATION_KIND_CHANNEL) {
        out.add(u);
      }
    }
    return out;
  }

  private List<ChatStores.BadgeRow> snapshot(String userId) {
    return rows(userId).stream().map(u -> new ChatStores.BadgeRow(u.convId(), u.lastSeq(), u.readSeq())).toList();
  }

  private int scan(String userId, String convId, long convSeq) {
    long sum = 0;
    boolean seen = false;
    for (ChatStores.UserConv u : rows(userId)) {
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

  @Override
  public void raise(String user, String convId, long last, long read) {
    write(List.of(user), () -> counters.raise(user, convId, last, read));
  }

  @Override
  public void raiseAll(List<String> users, String convId, long last) {
    if (!users.isEmpty()) {
      write(users, () -> counters.raiseAll(users, convId, last));
    }
  }

  @Override
  public void join(String user, String convId, long last) {
    write(List.of(user), () -> counters.join(user, convId, last));
  }

  @Override
  public void drop(String user, String convId) {
    write(List.of(user), () -> counters.drop(user, convId));
  }

  /**
   * Final review I2: the badge is secondary, so a Redis failure costs the hot loops one bounded wait, not one per
   * call. After a failure nothing is sent to Redis for {@link #PAUSE_MS}: badges are summed from the inbox, and the
   * users whose writes were lost are remembered (at most {@link #MAX_DIRTY}) and their counters deleted once Redis
   * answers (spec §3.3: never trust a counter that missed a write).
   */
  private void write(List<String> users, Runnable op) {
    if (counters == null) {
      return;
    }
    if (!usable()) {
      lost(users);
      return;
    }
    try {
      op.run();
    } catch (RuntimeException e) {
      pause();
      lost(users);
    }
  }

  /** Whether Redis may be called now; first deletes the counters that missed writes while paused. */
  private boolean usable() {
    if (wallMs.getAsLong() < pausedUntil) {
      return false;
    }
    if (!dirty.isEmpty()) {
      List<String> forget = List.copyOf(dirty);
      try {
        counters.forget(forget);
        forget.forEach(dirty::remove);
      } catch (RuntimeException e) {
        pause();
        return false;
      }
    }
    return true;
  }

  private void pause() {
    pausedUntil = wallMs.getAsLong() + PAUSE_MS;
  }

  private void lost(Collection<String> users) {
    if (dirty.size() + users.size() <= MAX_DIRTY) {
      dirty.addAll(users);
    }
    // past the bound the 24 h rebuild repairs what is left (spec §3.3)
  }
}
