package com.pangate.chat.core;

import com.pangate.contracts.v1.ChatCommand;

/** Writes a command to {@code chat.commands}, keyed by conversation; throws when Kafka did not take it. */
@FunctionalInterface
public interface CommandSink {
  void publish(ChatCommand command);
}
