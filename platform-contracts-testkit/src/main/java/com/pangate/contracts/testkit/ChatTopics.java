package com.pangate.contracts.testkit;

/** Topics and naming of the app chat (chat spec §1, §6, §8). */
public final class ChatTopics {

  public static final String COMMANDS = "chat.commands";
  public static final String EVENTS = "chat.events";
  public static final String DLQ = "chat.commands.dlq";
  /** The one PanGate tenant the app chat runs as. */
  public static final String TENANT = "chat";
  /** Principals of the chat tenant: {@code chat:<userId>}. */
  public static final String PRINCIPAL_PREFIX = "chat:";

  private ChatTopics() {}

  /** Broadcast topic of a channel: {@code chat:chan:<convId>}. */
  public static String channelTopic(String convId) {
    return "chat:chan:" + convId;
  }
}
