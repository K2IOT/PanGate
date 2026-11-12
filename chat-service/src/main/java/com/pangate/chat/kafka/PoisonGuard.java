package com.pangate.chat.kafka;

import com.pangate.chat.core.FencedException;
import com.pangate.chat.core.PushUnavailableException;
import com.pangate.chat.core.QueueUnavailableException;
import com.pangate.chat.core.StoreUnavailableException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;

/**
 * Poison records (chat spec §8): a record that fails the same deterministic way {@link #MAX_ATTEMPTS} times in a row
 * goes to the DLQ instead of holding its partition forever. Transient failures (the store, Kafka, Push API, an LWT
 * fence, a driver exception) are not counted: they are retried for as long as they last.
 */
final class PoisonGuard {

  static final int MAX_ATTEMPTS = 5;

  private record At(long offset, int failures) {}

  private final Map<TopicPartition, At> at = new ConcurrentHashMap<>();

  static boolean isTransient(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof StoreUnavailableException || t instanceof QueueUnavailableException
          || t instanceof FencedException || t instanceof PushUnavailableException
          || t instanceof InterruptedException) {
        return true;
      }
      String n = t.getClass().getName();
      if (n.startsWith("io.lettuce.") || n.startsWith("org.apache.kafka.") || n.startsWith("com.datastax.")
          || n.startsWith("io.grpc.")) {
        return true;
      }
    }
    return false;
  }

  /** Counts a failure of the record at {@code offset}; true once it failed {@link #MAX_ATTEMPTS} times in a row. */
  boolean failed(TopicPartition tp, long offset, Throwable e) {
    if (isTransient(e)) {
      return false;
    }
    At n = at.merge(tp, new At(offset, 1),
        (o, x) -> o.offset() == offset ? new At(offset, o.failures() + 1) : x);
    return n.failures() >= MAX_ATTEMPTS;
  }

  /** True when the record at {@code offset} already failed {@code MAX_ATTEMPTS - 1} times: this try is its last. */
  boolean lastTry(TopicPartition tp, long offset) {
    At a = at.get(tp);
    return a != null && a.offset() == offset && a.failures() >= MAX_ATTEMPTS - 1;
  }

  /** The partition got past the record it was counting. */
  void clear(TopicPartition tp) {
    at.remove(tp);
  }
}
