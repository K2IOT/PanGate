package com.pangate.contracts.testkit;

import com.pangate.contracts.v1.ChatSend;
import java.util.ArrayList;
import java.util.List;

/**
 * Validation cases for {@code ChatSend} (chat spec §4.3, the rules that need no stored state). chat-service must
 * answer each with {@code expectedReason}; empty means valid. Data only: the validator lives in chat-service.
 */
public final class ChatSendCases {

  public record Case(String name, ChatSend send, String expectedReason) {}

  public static final String CONV = "01JD0C0NV0000000000000000A";
  public static final String CLIENT_MSG = "01JD0M5G000000000000000000";

  private ChatSendCases() {}

  private static ChatSend.Builder valid() {
    return ChatSend.newBuilder().setConversationId(CONV).setClientMsgId(CLIENT_MSG).setText("hello");
  }

  public static List<Case> all() {
    List<Case> c = new ArrayList<>();
    c.add(new Case("valid", valid().build(), ""));
    c.add(new Case("empty conversation_id", valid().setConversationId("").build(), "invalid"));
    c.add(new Case("conversation_id not ULID", valid().setConversationId("conv-1").build(), "invalid"));
    c.add(new Case("client_msg_id not ULID", valid().setClientMsgId("m1").build(), "invalid"));
    c.add(new Case("empty text", valid().setText("").build(), "invalid"));
    c.add(new Case("text 4096 bytes", valid().setText("a".repeat(4096)).build(), ""));
    c.add(new Case("text 4097 bytes", valid().setText("a".repeat(4097)).build(), "too_long"));
    c.add(new Case("1024 four-byte emoji", valid().setText("😀".repeat(1024)).build(), ""));
    List<String> fifty = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      fifty.add("u" + i);
    }
    c.add(new Case("50 mentions", valid().addAllMentionUserIds(fifty).build(), ""));
    c.add(new Case("51 mentions", valid().addAllMentionUserIds(fifty).addMentionUserIds("u50").build(), "invalid"));
    c.add(new Case("reply_to not ULID", valid().setReplyToMsgId("x").build(), "invalid"));
    return List.copyOf(c);
  }
}
