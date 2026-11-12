package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.StoredMessage;

/** A command after sequencing: its message, whether an earlier send already produced it, and the HTTP reply target. */
public record SequencedMessage(ChatStores.StoredMessage message, boolean duplicate, String replyTo) {

  public SequencedMessage {
    replyTo = replyTo == null ? "" : replyTo;
  }

  static SequencedMessage of(StoredMessage m, boolean duplicate, String replyTo) {
    return new SequencedMessage(m, duplicate, replyTo);
  }
}
