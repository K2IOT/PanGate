package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.v1.ConversationKind;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConversationsTest {

  final FakeStores stores = new FakeStores();
  final List<String> unsubscribed = new ArrayList<>();
  final Conversations convs = new Conversations(stores, stores, new MemberLookup(stores, stores), FakeStores.LIMITS,
      (user, conv) -> unsubscribed.add(user + "@" + conv));

  static List<String> users(int n) {
    List<String> out = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      out.add("m" + i);
    }
    return out;
  }

  static String code(Runnable r) {
    try {
      r.run();
      return "";
    } catch (ChatRefusal e) {
      return e.code();
    }
  }

  @Test
  void createdGroupHasTheCreatorAsOwner() {
    Conversation c = convs.create("u1", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of("u2", "u1", "u2"),
        false);
    assertThat(stores.member(c.id(), "u1")).get().extracting(Member::role).isEqualTo(Role.OWNER);
    assertThat(stores.members(c.id())).hasSize(2);
    assertThat(c.createdBy()).isEqualTo("u1");
  }

  @Test
  void groupOver500IsRejected() {
    assertThat(code(() -> convs.create("u1", ConversationKind.CONVERSATION_KIND_GROUP, "g", users(499), false)))
        .isEmpty();
    assertThat(code(() -> convs.create("u1", ConversationKind.CONVERSATION_KIND_GROUP, "g", users(500), false)))
        .isEqualTo(ChatRefusal.INVALID);
    Conversation full = convs.create("u1", ConversationKind.CONVERSATION_KIND_GROUP, "g", users(499), false);
    assertThat(code(() -> convs.addMember("u1", full.id(), "late"))).isEqualTo(ChatRefusal.INVALID);
    assertThat(code(() -> convs.create("u1", ConversationKind.CONVERSATION_KIND_DIRECT, "g", List.of("u2"), false)))
        .as("DMs are opened, not created").isEqualTo(ChatRefusal.INVALID);
  }

  @Test
  void dmWithSelfIsInvalid() {
    assertThat(code(() -> convs.openDm("u1", "u1"))).isEqualTo(ChatRefusal.INVALID);
    assertThat(code(() -> convs.openDm("u1", ""))).isEqualTo(ChatRefusal.INVALID);
    Conversation a = convs.openDm("u1", "u2");
    Conversation b = convs.openDm("u2", "u1");
    assertThat(b.id()).isEqualTo(a.id());
    assertThat(a.kind()).isEqualTo(ConversationKind.CONVERSATION_KIND_DIRECT);
    assertThat(stores.members(a.id())).extracting(Member::userId).containsExactlyInAnyOrder("u1", "u2");
  }

  @Test
  void dmIndexWithoutConversationIsRepaired() {
    String orphan = Ids.next();
    stores.createDm("u1", "u3", orphan);   // a crash between dm_index and the conversation rows
    Conversation c = convs.openDm("u3", "u1");
    assertThat(c.id()).isEqualTo(orphan);
    assertThat(stores.get(orphan)).isPresent();
  }

  @Test
  void onlyOwnerOrAdminManagesMembers() {
    Conversation c = convs.create("own", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of("adm", "m1"), false);
    stores.add(c.id(), new Member("adm", Role.ADMIN));
    assertThat(code(() -> convs.addMember("m1", c.id(), "x"))).isEqualTo(ChatRefusal.FORBIDDEN);
    assertThat(code(() -> convs.addMember("stranger", c.id(), "x"))).isEqualTo(ChatRefusal.FORBIDDEN);
    assertThat(code(() -> convs.addMember("adm", c.id(), "x"))).isEmpty();
    assertThat(code(() -> convs.removeMember("m1", c.id(), "x"))).isEqualTo(ChatRefusal.FORBIDDEN);
    assertThat(code(() -> convs.removeMember("adm", c.id(), "own"))).as("an admin cannot remove the owner")
        .isEqualTo(ChatRefusal.FORBIDDEN);
    assertThat(code(() -> convs.removeMember("own", c.id(), "x"))).isEmpty();
    assertThat(stores.member(c.id(), "x")).isEmpty();
    assertThat(code(() -> convs.addMember("own", Ids.next(), "x"))).isEqualTo(ChatRefusal.NOT_FOUND);
    Conversation dm = convs.openDm("a", "b");
    assertThat(code(() -> convs.addMember("a", dm.id(), "c"))).isEqualTo(ChatRefusal.INVALID);
  }

  @Test
  void anyoneCanLeave() {
    Conversation c = convs.create("own", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of("m1"), false);
    stores.putMembers(c.id(), stores.members(c.id()));   // warm cache
    assertThat(code(() -> convs.removeMember("m1", c.id(), "m1"))).isEmpty();
    assertThat(stores.member(c.id(), "m1")).isEmpty();
    assertThat(stores.cache).doesNotContainKey(c.id());
  }

  @Test
  void joinPrivateChannelIsForbidden() {
    Conversation pub = convs.create("own", ConversationKind.CONVERSATION_KIND_CHANNEL, "c", List.of(), true);
    Conversation priv = convs.create("own", ConversationKind.CONVERSATION_KIND_CHANNEL, "c", List.of(), false);
    Conversation group = convs.create("own", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of(), false);
    assertThat(code(() -> convs.join("fan", pub.id()))).isEmpty();
    assertThat(stores.member(pub.id(), "fan")).get().extracting(Member::role).isEqualTo(Role.MEMBER);
    assertThat(code(() -> convs.join("fan", priv.id()))).isEqualTo(ChatRefusal.FORBIDDEN);
    assertThat(code(() -> convs.join("fan", group.id()))).isEqualTo(ChatRefusal.FORBIDDEN);
    assertThat(code(() -> convs.join("fan", Ids.next()))).isEqualTo(ChatRefusal.NOT_FOUND);
  }

  @Test
  void visibility() {
    Conversation pub = convs.create("own", ConversationKind.CONVERSATION_KIND_CHANNEL, "c", List.of(), true);
    Conversation group = convs.create("own", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of(), false);
    assertThat(convs.visible("anyone", pub.id()).id()).isEqualTo(pub.id());
    assertThat(convs.visible("own", group.id()).id()).isEqualTo(group.id());
    assertThat(code(() -> convs.visible("anyone", group.id()))).isEqualTo(ChatRefusal.FORBIDDEN);
    assertThat(code(() -> convs.visible("anyone", Ids.next()))).isEqualTo(ChatRefusal.NOT_FOUND);
  }

  /** Final review I5: if the cached member list cannot be dropped, the removal is not reported done. */
  @Test
  void removalFailsWhenTheCacheCannotBeEvicted() {
    Conversation c = convs.create("own", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of("m1"), false);
    stores.evictFails = true;
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> convs.removeMember("own", c.id(), "m1"))
        .isInstanceOf(StoreUnavailableException.class);
  }

  /** Final review I6: leaving a channel ends the open subscriptions too. */
  @Test
  void removalFromAChannelUnsubscribes() {
    Conversation ch = convs.create("own", ConversationKind.CONVERSATION_KIND_CHANNEL, "c", List.of("m1"), false);
    Conversation g = convs.create("own", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of("m1"), false);
    convs.removeMember("own", ch.id(), "m1");
    convs.removeMember("own", g.id(), "m1");
    assertThat(unsubscribed).containsExactly("m1@" + ch.id());
  }
}
