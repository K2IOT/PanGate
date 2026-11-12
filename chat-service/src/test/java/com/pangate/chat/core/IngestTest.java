package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.v1.ChatCommand;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.ConversationKind;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class IngestTest {

  final FakeStores stores = new FakeStores();
  final List<ChatCommand> published = new ArrayList<>();
  final AtomicLong nanos = new AtomicLong();
  final Ingest ingest = new Ingest(stores, stores, stores, published::add, SendValidatorTest.LIMITS, nanos::get,
      System::currentTimeMillis);

  String conv(ConversationKind kind, Member... ms) {
    String id = Ids.next();
    stores.create(new Conversation(id, kind, "t", ms[0].userId(), 0, false, ms.length), List.of(ms));
    return id;
  }

  static ChatSend send(String conv) {
    return ChatSend.newBuilder().setConversationId(conv).setClientMsgId(Ids.next()).setText("hi").build();
  }

  @Test
  void validSendPublishesCommandKeyedByConversation() {
    String c = conv(ConversationKind.CONVERSATION_KIND_GROUP, new Member("u1", Role.MEMBER), new Member("u2", Role.MEMBER));
    ChatSend s = send(c);
    assertThat(ingest.accept(new SendInput("u1", "d1", s, ""))).isEmpty();
    assertThat(published).singleElement().satisfies(cmd -> {
      assertThat(cmd.getSenderId()).isEqualTo("u1");
      assertThat(cmd.getSenderDeviceId()).isEqualTo("d1");
      assertThat(cmd.getSend()).isEqualTo(s);
      assertThat(Ids.isUlid(cmd.getCommandId())).isTrue();
      assertThat(cmd.getReceivedAtMs()).isPositive();
    });
  }

  @Test
  void notMemberIsRejectedAndNotPublished() {
    String c = conv(ConversationKind.CONVERSATION_KIND_GROUP, new Member("u1", Role.MEMBER));
    assertThat(ingest.accept(new SendInput("stranger", "d", send(c), ""))).get()
        .extracting(Rejection::reason).isEqualTo("not_member");
    assertThat(ingest.accept(new SendInput("u1", "d", send(Ids.next()), ""))).get()
        .extracting(Rejection::reason).isEqualTo("not_found");
    assertThat(published).isEmpty();
  }

  @Test
  void channelPostNeedsOwnerOrAdmin() {
    String c = conv(ConversationKind.CONVERSATION_KIND_CHANNEL, new Member("own", Role.OWNER),
        new Member("adm", Role.ADMIN), new Member("fan", Role.MEMBER));
    assertThat(ingest.accept(new SendInput("fan", "d", send(c), ""))).get()
        .extracting(Rejection::reason).isEqualTo("not_member");
    assertThat(ingest.accept(new SendInput("own", "d", send(c), ""))).isEmpty();
    assertThat(ingest.accept(new SendInput("adm", "d", send(c), ""))).isEmpty();
  }

  @Test
  void userLimit20PerSecondBurst40() {
    List<String> convs = new ArrayList<>();
    for (int i = 0; i < 41; i++) {   // one conversation each: only the user's own limit is in play
      convs.add(conv(ConversationKind.CONVERSATION_KIND_DIRECT, new Member("busy", Role.MEMBER)));
    }
    for (int i = 0; i < 40; i++) {
      assertThat(ingest.accept(new SendInput("busy", "d", send(convs.get(i)), ""))).as("send " + i).isEmpty();
    }
    assertThat(ingest.accept(new SendInput("busy", "d", send(convs.get(40)), ""))).get()
        .extracting(Rejection::reason).isEqualTo("rate_limited");
    nanos.addAndGet(1_000_000_000L / 20);   // one token back after 50 ms
    assertThat(ingest.accept(new SendInput("busy", "d", send(convs.get(40)), ""))).isEmpty();
  }

  @Test
  void conversationLimit50Burst100() {
    Member[] ms = new Member[6];
    for (int i = 0; i < ms.length; i++) {
      ms[i] = new Member("m" + i, Role.MEMBER);
    }
    String c = conv(ConversationKind.CONVERSATION_KIND_GROUP, ms);
    int ok = 0;
    for (int i = 0; i < 120; i++) {   // spread over 6 users: 6 × 40 burst > 100, so the conversation limit decides
      if (ingest.accept(new SendInput("m" + (i % 6), "d", send(c), "")).isEmpty()) {
        ok++;
      }
    }
    assertThat(ok).isEqualTo(100);
  }

  @Test
  void membersAreReadThroughTheCache() {
    String c = conv(ConversationKind.CONVERSATION_KIND_GROUP, new Member("u1", Role.MEMBER));
    ingest.accept(new SendInput("u1", "d", send(c), ""));
    assertThat(stores.cache).containsKey(c);
    stores.members.get(c).clear();   // cache still says u1 is a member
    assertThat(ingest.accept(new SendInput("u1", "d", send(c), ""))).isEmpty();
  }
}
