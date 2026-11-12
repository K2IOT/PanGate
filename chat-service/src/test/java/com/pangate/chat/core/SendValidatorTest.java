package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.config.ChatProperties;
import com.pangate.contracts.testkit.ChatSendCases;
import com.pangate.contracts.v1.ChatSend;
import org.junit.jupiter.api.Test;

class SendValidatorTest {

  static final ChatProperties.Limits LIMITS = new ChatProperties.Limits(20, 40, 50, 100, 500, 4096, 50);

  @Test
  void everyContractCase() {
    for (ChatSendCases.Case c : ChatSendCases.all()) {
      assertThat(SendValidator.check(c.send(), LIMITS).map(Rejection::reason).orElse("")).as(c.name())
          .isEqualTo(c.expectedReason());
    }
  }

  /** Review Focus 1: the limit counts UTF-8 bytes, not characters. */
  @Test
  void textLimitIsUtf8Bytes() {
    ChatSend.Builder b = ChatSend.newBuilder().setConversationId(Ids.next()).setClientMsgId(Ids.next());
    assertThat(SendValidator.check(b.setText("😀".repeat(1024)).build(), LIMITS)).isEmpty();
    assertThat(SendValidator.check(b.setText("😀".repeat(1024) + "a").build(), LIMITS))
        .get().extracting(Rejection::reason).isEqualTo("too_long");
  }
}
