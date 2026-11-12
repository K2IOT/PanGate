package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.SyncService.SyncCursor;
import com.pangate.chat.core.SyncService.SyncResult;
import com.pangate.contracts.v1.ConversationKind;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SyncTest {

  final FakeStores stores = new FakeStores();
  final SyncService sync = new SyncService(stores, stores, stores);

  String conv(ConversationKind kind, boolean isPublic, int messages, String... members) {
    String id = Ids.next();
    List<Member> ms = new ArrayList<>();
    for (String m : members) {
      ms.add(new Member(m, Role.MEMBER));
    }
    stores.create(new Conversation(id, kind, "t", members.length > 0 ? members[0] : "x", 0, isPublic, ms.size()), ms);
    for (int i = 1; i <= messages; i++) {
      stores.write(new StoredMessage(id, i, Ids.next(), "x", Ids.next(), "m" + i, "", List.of(), i));
    }
    stores.lastSeq.put(id, (long) messages);
    return id;
  }

  @Test
  void returnsMessagesAfterCursor() {
    String c = conv(ConversationKind.CONVERSATION_KIND_GROUP, false, 10, "u1");
    List<SyncResult> r = sync.sync("u1", List.of(new SyncCursor(c, 7)), 0);
    assertThat(r).singleElement().satisfies(x -> {
      assertThat(x.convId()).isEqualTo(c);
      assertThat(x.messages()).extracting(StoredMessage::convSeq).containsExactly(8L, 9L, 10L);
      assertThat(x.tooMany()).isFalse();
      assertThat(x.lastSeq()).isEqualTo(10);
    });
  }

  @Test
  void tooManyWhenGapExceedsLimit() {
    String c = conv(ConversationKind.CONVERSATION_KIND_GROUP, false, 60, "u1");
    assertThat(sync.sync("u1", List.of(new SyncCursor(c, 0)), 0)).singleElement().satisfies(x -> {
      assertThat(x.tooMany()).as("default 50 per conversation").isTrue();
      assertThat(x.messages()).isEmpty();
      assertThat(x.lastSeq()).isEqualTo(60);
    });
    assertThat(sync.sync("u1", List.of(new SyncCursor(c, 0)), 100).getFirst().messages()).hasSize(60);
    assertThat(sync.sync("u1", List.of(new SyncCursor(c, 10)), 50).getFirst().messages()).hasSize(50);
  }

  /** Review Focus 3: a cursor ahead of the conversation (corrupt client data, re-created conversation). */
  @Test
  void clientAheadGetsNothing() {
    String c = conv(ConversationKind.CONVERSATION_KIND_GROUP, false, 5, "u1");
    assertThat(sync.sync("u1", List.of(new SyncCursor(c, 99)), 0)).singleElement().satisfies(x -> {
      assertThat(x.messages()).isEmpty();
      assertThat(x.tooMany()).isFalse();
      assertThat(x.lastSeq()).isEqualTo(5);
    });
  }

  @Test
  void notMemberIsSkipped() {
    String mine = conv(ConversationKind.CONVERSATION_KIND_GROUP, false, 2, "u1");
    String theirs = conv(ConversationKind.CONVERSATION_KIND_GROUP, false, 2, "u2");
    String publicChannel = conv(ConversationKind.CONVERSATION_KIND_CHANNEL, true, 3, "owner");
    String privateChannel = conv(ConversationKind.CONVERSATION_KIND_CHANNEL, false, 3, "owner");
    List<SyncResult> r = sync.sync("u1", List.of(new SyncCursor(mine, 0), new SyncCursor(theirs, 0),
        new SyncCursor(Ids.next(), 0), new SyncCursor(publicChannel, 1), new SyncCursor(privateChannel, 0)), 0);
    assertThat(r).extracting(SyncResult::convId).containsExactly(mine, publicChannel);
    assertThat(r.get(1).messages()).extracting(StoredMessage::convSeq).containsExactly(2L, 3L);
  }

  @Test
  void limitsAreEnforced() {
    List<SyncCursor> many = new ArrayList<>();
    for (int i = 0; i < 101; i++) {
      many.add(new SyncCursor(Ids.next(), 0));
    }
    assertThatThrownBy(() -> sync.sync("u1", many, 0)).isInstanceOf(ChatRefusal.class);
    assertThatThrownBy(() -> sync.sync("u1", List.of(), 101)).isInstanceOf(ChatRefusal.class);
    assertThatThrownBy(() -> sync.sync("u1", List.of(new SyncCursor(Ids.next(), -1)), 0))
        .isInstanceOf(ChatRefusal.class);
  }
}
