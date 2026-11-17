package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Marks;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.ReceiptCommand;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** CH3 spec §3.4: marks are merged per (conversation, user) and flushed per conversation window. */
class ReceiptAggregatorTest {

  final AtomicLong ms = new AtomicLong();
  final ReceiptAggregator agg = new ReceiptAggregator(ms::get, Duration.ofMillis(500), Duration.ofSeconds(2));

  static ReceiptCommand mark(String conv, String user, long delivered, long read) {
    return ReceiptCommand.newBuilder().setConversationId(conv).setUserId(user).setDeliveredSeq(delivered)
        .setReadSeq(read).build();
  }

  @Test
  void keepsTheMaxOutOfOrder() {
    agg.add(mark("g", "u", 5, 5), ConversationKind.CONVERSATION_KIND_GROUP);
    agg.add(mark("g", "u", 3, 3), ConversationKind.CONVERSATION_KIND_GROUP);
    agg.add(mark("g", "u", 4, 0), ConversationKind.CONVERSATION_KIND_GROUP);
    agg.add(mark("g", "v", 2, 0), ConversationKind.CONVERSATION_KIND_GROUP);
    ms.set(2_000);
    assertThat(agg.due()).singleElement().satisfies(f -> {
      assertThat(f.convId()).isEqualTo("g");
      assertThat(f.byUser()).containsEntry("u", new Marks(5, 5)).containsEntry("v", new Marks(2, 0));
    });
    assertThat(agg.due()).isEmpty();
  }

  @Test
  void readRaisesDelivered() {
    agg.add(mark("d", "u", 2, 6), ConversationKind.CONVERSATION_KIND_DIRECT);
    ms.set(500);
    assertThat(agg.due()).singleElement().satisfies(f -> assertThat(f.byUser()).containsEntry("u", new Marks(6, 6)));
  }

  @Test
  void dmFlushesAfter500ms() {
    agg.add(mark("d", "u", 1, 1), ConversationKind.CONVERSATION_KIND_DIRECT);
    ms.set(499);
    assertThat(agg.due()).isEmpty();
    ms.set(500);
    assertThat(agg.due()).hasSize(1);
  }

  @Test
  void groupFlushesAfter2s() {
    agg.add(mark("g", "u", 1, 1), ConversationKind.CONVERSATION_KIND_GROUP);
    ms.set(1_999);
    assertThat(agg.due()).isEmpty();
    ms.set(2_000);
    assertThat(agg.due()).hasSize(1);
  }

  @Test
  void channelFlushesAtOnce() {
    agg.add(mark("c", "u", 1, 1), ConversationKind.CONVERSATION_KIND_CHANNEL);
    assertThat(agg.due()).singleElement().satisfies(f -> assertThat(f.kind())
        .isEqualTo(ConversationKind.CONVERSATION_KIND_CHANNEL));
  }

  @Test
  void pendingTellsWhetherAConversationWaits() {
    agg.add(mark("g", "u", 1, 1), ConversationKind.CONVERSATION_KIND_GROUP);
    assertThat(agg.pending("g")).isTrue();
    assertThat(agg.pending("x")).isFalse();
    agg.clear();
    assertThat(agg.pending("g")).isFalse();
  }
}
