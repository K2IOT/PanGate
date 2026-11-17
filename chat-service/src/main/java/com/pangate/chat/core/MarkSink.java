package com.pangate.chat.core;

import com.pangate.contracts.v1.ReceiptCommand;

/** Where accepted marks go: {@code chat.receipts}, key conversation id (CH3 spec §3.3). */
public interface MarkSink {
  /** Throws {@link QueueUnavailableException} when the queue did not take it. */
  void publish(ReceiptCommand c);
}
