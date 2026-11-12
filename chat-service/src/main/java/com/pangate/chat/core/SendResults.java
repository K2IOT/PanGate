package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.StoredMessage;
import java.time.Duration;
import java.util.Optional;

/**
 * The result of an HTTP send travels back to the pod that took it (chat spec §4.2): the sequencer stores it under
 * {@link #key} and notifies {@code sendres:<pod>}; the pod waits for it. Only ids, seq and sent-at are carried.
 */
public interface SendResults {

  /** No waiting: every HTTP send answers 202 (Redis unusable, or in tests). */
  SendResults NONE = new SendResults() {
    @Override
    public void publish(String replyTo, StoredMessage m) {}

    @Override
    public Optional<StoredMessage> await(String key, Duration wait) {
      return Optional.empty();
    }
  };

  static String key(String convId, String senderId, String clientMsgId) {
    return "sendres:{" + convId + "}:" + senderId + ":" + clientMsgId;
  }

  /** {@code replyTo} is {@code http:<pod>}; never throws. */
  void publish(String replyTo, StoredMessage m);

  /** The result if it arrives within {@code wait}; empty on timeout or when Redis cannot be used. */
  Optional<StoredMessage> await(String key, Duration wait);
}
