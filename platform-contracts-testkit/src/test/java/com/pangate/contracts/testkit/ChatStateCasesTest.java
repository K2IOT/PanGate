package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/** CH3 spec §2, §3.3, §6.3: the cases chat-service validates marks and watches against. */
class ChatStateCasesTest {

  @Test
  void invalidMarksCoverEveryRule() {
    List<String> names = ChatStateCases.invalidMarks().stream().map(ChatStateCases.MarkCase::name).toList();
    assertThat(names).contains("conversation_id not ULID", "up_to_seq 0");
    assertThat(ChatStateCases.invalidMarks()).allSatisfy(c -> assertThat(c.reason()).isEqualTo("invalid"));
    assertThat(ChatStateCases.validMarks()).isNotEmpty().allSatisfy(c -> assertThat(c.reason()).isEmpty());
  }

  @Test
  void invalidWatchesCoverEveryRule() {
    List<String> names = ChatStateCases.invalidWatches().stream().map(ChatStateCases.WatchCase::name).toList();
    assertThat(names).contains("201 entries", "empty user_id", "conversation_id not ULID");
    assertThat(ChatStateCases.invalidWatches().stream().filter(c -> c.name().equals("201 entries")).findFirst()
        .orElseThrow().watch().getEntriesCount()).isEqualTo(201);
    assertThat(ChatStateCases.validWatches()).isNotEmpty();
  }

  @Test
  void receiptsTopicNameIsPinned() {
    assertThat(ChatTopics.RECEIPTS).isEqualTo("chat.receipts");
    assertThat(List.of(ChatTopics.OP_DELIVERED, ChatTopics.OP_READ, ChatTopics.OP_TYPING, ChatTopics.OP_WATCH))
        .containsExactly("chat.delivered", "chat.read", "chat.typing", "chat.presence.watch");
    assertThat(List.of(ChatTopics.TYPE_RECEIPTS, ChatTopics.TYPE_TYPING, ChatTopics.TYPE_PRESENCE))
        .containsExactly("chat.receipts", "chat.typing", "chat.presence");
  }
}
