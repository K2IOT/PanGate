package com.pangate.chat.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ChatConfigTest {

  /** Final review minor: one part that fails to close must not leave the parts after it open. */
  @Test
  void aFailingCloseDoesNotSkipTheRest() {
    List<String> closed = new ArrayList<>();
    ChatConfig.closeInOrder(
        () -> closed.add("a"),
        () -> {
          throw new IllegalStateException("boom");
        },
        () -> closed.add("c"));
    assertThat(closed).containsExactly("a", "c");
  }
  /** Codex review P2: an overridden commands or DLQ topic is where the producer writes, not the default. */
  @Test
  void commandsAndDeadLettersGoToTheConfiguredTopics() {
    ChatProperties p = new org.springframework.boot.context.properties.bind.Binder(
        new org.springframework.boot.context.properties.source.MapConfigurationPropertySource(java.util.Map.of(
            "chat.cell", "c1", "chat.kafka.bootstrap", "127.0.0.1:1", "chat.kafka.commands-topic", "x.cmds",
            "chat.kafka.dlq-topic", "x.dlq", "chat.kafka.produce-timeout", "200ms")))
        .bind("chat", ChatProperties.class).get();
    try (com.pangate.chat.kafka.CommandProducer c = new ChatConfig().commandProducer(p)) {
      com.pangate.contracts.v1.ChatCommand cmd = com.pangate.contracts.v1.ChatCommand.newBuilder()
          .setSend(com.pangate.contracts.v1.ChatSend.newBuilder().setConversationId("c")).build();
      org.assertj.core.api.Assertions.assertThatThrownBy(() -> c.publish(cmd)).hasMessageContaining("x.cmds");
      org.assertj.core.api.Assertions.assertThatThrownBy(() -> c.deadLetter("k", new byte[0], "r"))
          .hasMessageContaining("x.dlq");
    }
  }
}
