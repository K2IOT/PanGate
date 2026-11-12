package com.pangate.chat.core;

/** Kafka did not acknowledge a record in time (chat spec §8): the operation may be retried. */
public final class QueueUnavailableException extends RuntimeException {
  public QueueUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}
