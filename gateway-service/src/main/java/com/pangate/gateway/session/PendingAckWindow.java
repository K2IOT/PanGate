package com.pangate.gateway.session;

import com.pangate.gateway.downstream.StreamRef;
import java.util.function.Consumer;

/**
 * Pushes awaiting ACK on one session (spec §5.4): at most {@code maxMsgs} entries and {@code maxBytes}, kept in send
 * order (so the oldest is first); lookups scan linearly (n ≤ 32). Owner loop only.
 */
public final class PendingAckWindow {

  private final PendingEntry[] entries;
  private final int maxBytes;
  private int size;
  private int bytes;

  public PendingAckWindow(int maxMsgs, int maxBytes) {
    this.entries = new PendingEntry[maxMsgs];
    this.maxBytes = maxBytes;
  }

  /** Room for one more entry of {@code n} bytes; an empty window takes any single frame. */
  public boolean fits(int n) {
    return size == 0 || (size < entries.length && bytes + n <= maxBytes);
  }

  public PendingEntry find(String msgId) {
    int i = indexOf(msgId);
    return i < 0 ? null : entries[i];
  }

  public void add(PendingEntry e) {
    entries[size++] = e;
    bytes += e.bytes();
  }

  public PendingEntry remove(String msgId) {
    int i = indexOf(msgId);
    if (i < 0) {
      return null;
    }
    PendingEntry e = entries[i];
    removeAt(i);
    return e;
  }

  /** The router sent the same message again: acks go to the newest delivery id and stream. */
  public void replace(String msgId, long deliveryId, StreamRef s) {
    int i = indexOf(msgId);
    if (i >= 0) {
      PendingEntry e = entries[i];
      entries[i] = new PendingEntry(msgId, deliveryId, s, e.sentAtSec(), e.bytes());
    }
  }

  public long oldestSentAtSec() {
    return size == 0 ? Long.MAX_VALUE : entries[0].sentAtSec();
  }

  /** Removes every entry sent at or before {@code nowSec - timeoutSec}, oldest first. */
  public void expire(long nowSec, long timeoutSec, Consumer<PendingEntry> out) {
    int n = 0;
    while (n < size && entries[n].sentAtSec() + timeoutSec <= nowSec) {
      n++;
    }
    for (int i = 0; i < n; i++) {
      out.accept(entries[i]);
    }
    shift(n);
  }

  public void drain(Consumer<PendingEntry> out) {
    for (int i = 0; i < size; i++) {
      out.accept(entries[i]);
    }
    shift(size);
  }

  public int size() {
    return size;
  }

  private int indexOf(String msgId) {
    for (int i = 0; i < size; i++) {
      if (entries[i].msgId().equals(msgId)) {
        return i;
      }
    }
    return -1;
  }

  private void removeAt(int i) {
    bytes -= entries[i].bytes();
    System.arraycopy(entries, i + 1, entries, i, size - i - 1);
    entries[--size] = null;
  }

  private void shift(int n) {
    if (n == 0) {
      return;
    }
    for (int i = 0; i < n; i++) {
      bytes -= entries[i].bytes();
    }
    System.arraycopy(entries, n, entries, 0, size - n);
    java.util.Arrays.fill(entries, size - n, size, null);
    size -= n;
  }
}
