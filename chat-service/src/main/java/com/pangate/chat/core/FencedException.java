package com.pangate.chat.core;

/**
 * The LWT on {@code conversations.last_seq} failed: another sequencer wrote to the conversation (a rebalance). The
 * batch is dropped and re-read from Kafka (chat spec §4.4 step 2).
 */
public final class FencedException extends RuntimeException {

  public FencedException(String convId) {
    super("last_seq of " + convId + " moved elsewhere", null, false, false);
  }
}
