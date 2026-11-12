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
}
