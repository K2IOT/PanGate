package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ChatSendCasesTest {

  static ChatSendCases.Case named(String name) {
    return ChatSendCases.all().stream().filter(c -> c.name().equals(name)).findFirst().orElseThrow();
  }

  @Test
  void casesCoverEveryRule() {
    Set<String> reasons = ChatSendCases.all().stream().map(ChatSendCases.Case::expectedReason)
        .collect(Collectors.toSet());
    assertThat(reasons).contains("", "invalid", "too_long");
    assertThat(named("text 4096 bytes").expectedReason()).isEmpty();
    assertThat(named("text 4097 bytes").expectedReason()).isEqualTo("too_long");
    assertThat(named("1024 four-byte emoji").expectedReason()).isEmpty();
    assertThat(named("51 mentions").expectedReason()).isEqualTo("invalid");
    assertThat(named("client_msg_id not ULID").expectedReason()).isEqualTo("invalid");
    assertThat(named("empty conversation_id").expectedReason()).isEqualTo("invalid");
    assertThat(named("empty text").expectedReason()).isEqualTo("invalid");
  }

  @Test
  void channelTopicNaming() {
    assertThat(ChatTopics.channelTopic("01JCHAN")).isEqualTo("chat:chan:01JCHAN");
    assertThat(ChatTopics.COMMANDS).isEqualTo("chat.commands");
    assertThat(ChatTopics.EVENTS).isEqualTo("chat.events");
    assertThat(ChatTopics.DLQ).isEqualTo("chat.commands.dlq");
    assertThat(ChatTopics.TENANT).isEqualTo("chat");
    assertThat(ChatTopics.PRINCIPAL_PREFIX).isEqualTo("chat:");
  }
}
