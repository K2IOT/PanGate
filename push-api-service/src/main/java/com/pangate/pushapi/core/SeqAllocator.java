package com.pangate.pushapi.core;

import com.pangate.contracts.testkit.SeqKeys;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code INCR seq:{p}} / {@code topicseq:{t}} plus the slot's epoch (push-api spec §3.1). A seq that does not grow
 * for a principal this pod already served means the state was reset (failover): the epoch is refreshed before the seq
 * is handed out, and a fresh seq is taken.
 */
public final class SeqAllocator {

  static final int REMEMBERED = 100_000;

  private final SeqStore store;
  private final SeqEpochs epochs;
  private final Runnable onRegression;
  private final Map<String, Long> highest = new LinkedHashMap<>(1024, 0.75f, true) {
    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Long> e) {
      return size() > REMEMBERED;
    }
  };

  public SeqAllocator(SeqStore store, SeqEpochs epochs, Runnable onRegression) {
    this.store = store;
    this.epochs = epochs;
    this.onRegression = onRegression;
  }

  public Seq forPrincipal(String principal) {
    String key = SeqKeys.seq(principal);
    long seq = store.incr(key);
    if (!remember(principal, seq)) {
      onRegression.run();     // the watcher bumps the shard's epoch if run_id changed
      epochs.refreshNow();
      seq = store.incr(key);
      synchronized (highest) {
        highest.put(principal, seq);
      }
    }
    return new Seq(seq, epochs.epoch(principal));
  }

  public long forTopic(String topic) {
    return store.incr(PushApiKeys.topicSeq(topic));
  }

  /** False when {@code seq} is not above the highest seq this pod handed out for the principal. */
  private boolean remember(String principal, long seq) {
    synchronized (highest) {
      Long prev = highest.get(principal);
      if (prev != null && seq <= prev) {
        return false;
      }
      highest.put(principal, seq);
      return true;
    }
  }
}
