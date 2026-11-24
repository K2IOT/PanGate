package com.pangate.chat.core;

import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import java.util.concurrent.CompletableFuture;

/**
 * Sends to the Push API. Asynchronous: a group message is up to 500 requests, sent together and awaited together.
 * The future completes with the Push API's answer (any code), or exceptionally with
 * {@link PushUnavailableException} when no answer came (stream down, timeout).
 */
@FunctionalInterface
public interface Pusher {

  /**
   * Perf 3b (spec §4.1): what happens when the Push API is busy. MUST (messages, channel posts, channel control) is
   * never dropped by chat for lack of room; BEST_EFFORT (receipts, send rejections) waits a little, then is dropped;
   * DROPPABLE (typing, presence) is dropped at once. A dropped push fails with {@link PushDroppedException}.
   */
  enum Priority { MUST, BEST_EFFORT, DROPPABLE }

  CompletableFuture<PushResponse> send(PushRequest r, Priority p);

  /** A push that must arrive. */
  default CompletableFuture<PushResponse> send(PushRequest r) {
    return send(r, Priority.MUST);
  }
}
