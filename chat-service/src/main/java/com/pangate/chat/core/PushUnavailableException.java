package com.pangate.chat.core;

/** No answer from the Push API (stream ended, timeout): retry later. */
public final class PushUnavailableException extends RuntimeException {

  public PushUnavailableException(String message, Throwable cause) {
    super(message, cause, false, false);
  }
}
