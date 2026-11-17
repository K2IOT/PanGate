package com.pangate.contracts.testkit;

/** Topics and naming of the app chat (chat spec §1, §6, §8). */
public final class ChatTopics {

  public static final String COMMANDS = "chat.commands";
  public static final String EVENTS = "chat.events";
  public static final String DLQ = "chat.commands.dlq";
  /** Delivered / read marks, key {@code conversation_id} (CH3 spec §3.3, §9). */
  public static final String RECEIPTS = "chat.receipts";
  /** Upstream ops of conversation state (CH3 spec §2). */
  public static final String OP_DELIVERED = "chat.delivered";
  public static final String OP_READ = "chat.read";
  public static final String OP_TYPING = "chat.typing";
  public static final String OP_WATCH = "chat.presence.watch";
  /** {@code headers.type} of conversation-state pushes (CH3 spec §2). */
  public static final String TYPE_RECEIPTS = "chat.receipts";
  public static final String TYPE_TYPING = "chat.typing";
  public static final String TYPE_PRESENCE = "chat.presence";
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
