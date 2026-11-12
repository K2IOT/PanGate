package com.pangate.contracts.v1;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** chat.proto field numbers are wire contract (chat spec §3): changing one breaks every client and topic. */
class ChatContractNumbersTest {

  @Test
  void fieldNumbersArePinned() {
    assertThat(ChatSend.TEXT_FIELD_NUMBER).isEqualTo(3);
    assertThat(ChatMessage.CONV_SEQ_FIELD_NUMBER).isEqualTo(3);
    assertThat(ChatMessage.SENT_AT_MS_FIELD_NUMBER).isEqualTo(9);
    assertThat(ChatCommand.SEND_FIELD_NUMBER).isEqualTo(10);
    assertThat(ChatCommand.REPLY_TO_FIELD_NUMBER).isEqualTo(20);
    assertThat(ConversationKind.CONVERSATION_KIND_CHANNEL_VALUE).isEqualTo(3);
  }
}
