package com.pangate.chat.core;

import com.pangate.contracts.v1.ChatRejected;

/** Tells the sender's devices that a WebSocket send was refused (chat spec §4.1 step 3; Push API, Task 6). */
@FunctionalInterface
public interface Notifier {
  void rejected(String senderId, ChatRejected r);
}
