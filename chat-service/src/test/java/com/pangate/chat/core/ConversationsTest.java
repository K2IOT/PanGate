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
    Conversation full = convs.create("u1", ConversationKind.CONVERSATION_KIND_GROUP, "g", users(199), false);
    for (int i = 199; i < 499; i++) {
      convs.addMember("u1", full.id(), "m" + i);
    }
    assertThat(stores.members(full.id())).hasSize(500);
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
    // Codex review P1: fail closed — nothing applied while a stale cached list could survive; the retry removes
    assertThat(stores.member(c.id(), "m1")).isPresent();
    stores.evictFails = false;
    convs.removeMember("own", c.id(), "m1");
    assertThat(stores.member(c.id(), "m1")).isEmpty();
  }

  /** Codex review P1: the cache is dropped before the change too, so a failing Redis leaves nothing half done. */
  @Test
  void additionAndChannelLeaveAreNotAppliedWhenTheCacheCannotBeEvicted() {
    Conversation g = convs.create("own", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of(), false);
    Conversation ch = convs.create("own", ConversationKind.CONVERSATION_KIND_CHANNEL, "c", List.of("m1"), false);
    stores.evictFails = true;
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> convs.addMember("own", g.id(), "m2"))
        .isInstanceOf(StoreUnavailableException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> convs.removeMember("m1", ch.id(), "m1"))
        .isInstanceOf(StoreUnavailableException.class);
    assertThat(stores.member(g.id(), "m2")).isEmpty();
    assertThat(stores.member(ch.id(), "m1")).isPresent();
    assertThat(unsubscribed).isEmpty();
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
  /** account spec §8: only users account-service knows, and not deleted ones, join conversations. */
  @Test
  void unknownOrDeletedUserCannotBeAdded() {
    Conversations checked = new Conversations(stores, stores, new MemberLookup(stores, stores), FakeStores.LIMITS,
        (user, conv) -> { }, new UserDirectory(stores));
    stores.upsertIfNewer("own", "active", 1);
    stores.upsertIfNewer("live", "active", 1);
    stores.upsertIfNewer("gone", "deleted", 1);
    assertThat(code(() -> checked.openDm("own", "nobody"))).isEqualTo("user_not_found");
    assertThat(code(() -> checked.openDm("own", "gone"))).isEqualTo("user_deleted");
    assertThat(code(() -> checked.create("own", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of("live",
        "nobody"), false))).isEqualTo("user_not_found");
    Conversation g = checked.create("own", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of("live"), false);
    assertThat(code(() -> checked.addMember("own", g.id(), "gone"))).isEqualTo("user_deleted");
    assertThat(code(() -> checked.openDm("own", "live"))).isEmpty();
  }

  /** Review 2026-11-19 (F2): an owner who leaves hands ownership on first, as a deleted account does. */
  @Test
  void anOwnerWhoLeavesHandsOwnershipOn() {
    Conversation c = convs.create("own", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of("m1", "adm"), false);
    stores.setRole(c.id(), "adm", Role.ADMIN);
    assertThat(code(() -> convs.removeMember("own", c.id(), "own"))).isEmpty();
    assertThat(stores.member(c.id(), "own")).isEmpty();
    assertThat(stores.member(c.id(), "adm")).get().extracting(Member::role).isEqualTo(Role.OWNER);

    Conversation ch = convs.create("own", ConversationKind.CONVERSATION_KIND_CHANNEL, "c", List.of("f1", "f2"), true);
    assertThat(code(() -> convs.removeMember("own", ch.id(), "own"))).isEmpty();
    assertThat(stores.member(ch.id(), "f1")).as("no admin: the earliest member").get().extracting(Member::role)
        .isEqualTo(Role.OWNER);

    Conversation alone = convs.create("own", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of(), false);
    assertThat(code(() -> convs.removeMember("own", alone.id(), "own"))).as("the last one just leaves").isEmpty();
    assertThat(stores.members(alone.id())).isEmpty();
  }

  /** Review 2026-11-19 (F2): only the owner makes or unmakes admins. */
  @Test
  void theOwnerSetsAdmins() {
    Conversation c = convs.create("own", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of("m1", "m2"), false);
    assertThat(code(() -> convs.setRole("m1", c.id(), "m2", Role.ADMIN))).isEqualTo(ChatRefusal.FORBIDDEN);
    assertThat(code(() -> convs.setRole("own", c.id(), "m1", Role.ADMIN))).isEmpty();
    assertThat(stores.member(c.id(), "m1")).get().extracting(Member::role).isEqualTo(Role.ADMIN);
    assertThat(code(() -> convs.setRole("m1", c.id(), "m2", Role.ADMIN))).as("an admin does not make admins")
        .isEqualTo(ChatRefusal.FORBIDDEN);
    assertThat(code(() -> convs.setRole("own", c.id(), "own", Role.MEMBER))).as("the owner's role moves by transfer")
        .isEqualTo(ChatRefusal.INVALID);
    assertThat(code(() -> convs.setRole("own", c.id(), "stranger", Role.ADMIN))).isEqualTo(ChatRefusal.NOT_FOUND);
    assertThat(code(() -> convs.setRole("own", c.id(), "m1", Role.OWNER))).isEqualTo(ChatRefusal.INVALID);
    assertThat(code(() -> convs.setRole("own", c.id(), "m1", Role.MEMBER))).isEmpty();
    assertThat(stores.member(c.id(), "m1")).get().extracting(Member::role).isEqualTo(Role.MEMBER);
    Conversation dm = convs.openDm("a", "b");
    assertThat(code(() -> convs.setRole("a", dm.id(), "b", Role.ADMIN))).isEqualTo(ChatRefusal.INVALID);
  }

  /** Review 2026-11-19 (F2): the owner hands ownership to a member and stays on as admin. */
  @Test
  void theOwnerTransfersOwnership() {
    Conversation c = convs.create("own", ConversationKind.CONVERSATION_KIND_CHANNEL, "c", List.of("m1"), false);
    assertThat(code(() -> convs.transferOwnership("m1", c.id(), "m1"))).isEqualTo(ChatRefusal.FORBIDDEN);
    assertThat(code(() -> convs.transferOwnership("own", c.id(), "stranger"))).isEqualTo(ChatRefusal.NOT_FOUND);
    assertThat(code(() -> convs.transferOwnership("own", c.id(), "own"))).isEqualTo(ChatRefusal.INVALID);
    assertThat(code(() -> convs.transferOwnership("own", c.id(), "m1"))).isEmpty();
    assertThat(stores.member(c.id(), "m1")).get().extracting(Member::role).isEqualTo(Role.OWNER);
    assertThat(stores.member(c.id(), "own")).get().extracting(Member::role).isEqualTo(Role.ADMIN);
    assertThat(stores.members(c.id()).stream().filter(m -> m.role() == Role.OWNER)).hasSize(1);
  }

  /** Review 2026-11-19 (F7): one create puts at most 200 others in. */
  @Test
  void createTakesAtMost200Others() {
    assertThat(code(() -> convs.create("u1", ConversationKind.CONVERSATION_KIND_CHANNEL, "c", users(200), true)))
        .isEmpty();
    assertThat(code(() -> convs.create("u1", ConversationKind.CONVERSATION_KIND_CHANNEL, "c", users(201), true)))
        .isEqualTo(ChatRefusal.INVALID);
  }

  /** Review 2026-11-19 (F7): someone whose whoCanAdd is nobody is never put into a group or channel by others. */
  @Test
  void whoRefusesAddsIsNotAdded() {
    Conversations checked = new Conversations(stores, stores, new MemberLookup(stores, stores), FakeStores.LIMITS,
        (user, conv) -> { }, UserDirectory.ANY, stores::privacy);
    stores.updatePrivacy("shy", null, null, null, true, 1);
    Conversations.Created c = checked.createReporting("own", ConversationKind.CONVERSATION_KIND_GROUP, "g",
        List.of("ok", "shy"), false);
    assertThat(c.notAdded()).containsExactly("shy");
    assertThat(stores.member(c.conversation().id(), "shy")).isEmpty();
    assertThat(stores.member(c.conversation().id(), "ok")).isPresent();
    assertThat(code(() -> checked.addMember("own", c.conversation().id(), "shy"))).isEqualTo(ChatRefusal.ADD_REFUSED);
    Conversation pub = checked.create("own", ConversationKind.CONVERSATION_KIND_CHANNEL, "c", List.of(), true);
    assertThat(code(() -> checked.join("shy", pub.id()))).as("they still follow public channels themselves")
        .isEmpty();
    stores.updatePrivacy("shy", null, null, null, false, 2);
    assertThat(code(() -> checked.addMember("own", c.conversation().id(), "shy"))).isEmpty();
  }

  long owners(String convId) {
    return stores.members(convId).stream().filter(m -> m.role() == Role.OWNER).count();
  }

  /** Batch 1 review I1: two transfers racing (two devices) leave exactly one owner. */
  @Test
  void racingTransfersLeaveOneOwner() {
    Conversation c = convs.create("own", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of("b", "c"), false);
    List<String> inner = new ArrayList<>();
    stores.beforeSetRole = (conv, user) -> inner.add(code(() -> convs.transferOwnership("own", c.id(), "c")));
    assertThat(code(() -> convs.transferOwnership("own", c.id(), "b"))).isEmpty();
    assertThat(inner).containsExactly(ChatRefusal.FORBIDDEN);
    assertThat(owners(c.id())).isEqualTo(1);
    assertThat(stores.member(c.id(), "b")).get().extracting(Member::role).isEqualTo(Role.OWNER);
  }

  /** Batch 1 review I1: the new owner leaving in the middle of a transfer does not leave the group without one. */
  @Test
  void aTransferToSomeoneLeavingStillLeavesAnOwner() {
    Conversation c = convs.create("own", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of("b", "c"), false);
    stores.beforeSetRole = (conv, user) -> convs.removeMember(user, conv, user);   // b leaves meanwhile
    convs.transferOwnership("own", c.id(), "b");
    assertThat(stores.member(c.id(), "b")).isEmpty();
    assertThat(owners(c.id())).isEqualTo(1);
    // b was owner when it left, so it handed ownership on; a manager of the group remains either way
    assertThat(code(() -> convs.addMember(stores.members(c.id()).stream().filter(m -> m.role() == Role.OWNER)
        .findFirst().orElseThrow().userId(), c.id(), "d"))).isEmpty();
  }

  /** Perf 3a (spec §3.3): joining a group with history counts nothing unread; the counter moves after the row. */
  @Test
  void addMemberRaisesBadgeAtLastSeq() {
    RecordingBadges b = new RecordingBadges();
    Conversations c = new Conversations(stores, stores, new MemberLookup(stores, stores), FakeStores.LIMITS,
        (user, conv) -> { }, UserDirectory.ANY, u -> ChatStores.Privacy.OPEN, b);
    Conversation g = c.create("u1", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of("u2"), false);
    stores.lastSeq.put(g.id(), 100L);
    b.onCall = call -> assertThat(stores.members.get(g.id())).containsKey("u3");
    c.addMember("u1", g.id(), "u3");
    assertThat(b.calls).containsExactly("join u3 " + g.id() + " 100");
  }

  @Test
  void leavingDropsBadge() {
    RecordingBadges b = new RecordingBadges();
    Conversations c = new Conversations(stores, stores, new MemberLookup(stores, stores), FakeStores.LIMITS,
        (user, conv) -> { }, UserDirectory.ANY, u -> ChatStores.Privacy.OPEN, b);
    Conversation g = c.create("u1", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of("u2", "u3"), false);
    c.removeMember("u2", g.id(), "u2");
    c.removeMember("u1", g.id(), "u3");
    assertThat(b.calls).containsExactly("drop u2 " + g.id(), "drop u3 " + g.id());
    Conversation ch = c.create("u1", ConversationKind.CONVERSATION_KIND_CHANNEL, "c", List.of("u2"), true);
    stores.lastSeq.put(ch.id(), 5L);
    c.addMember("u1", ch.id(), "u4");
    c.removeMember("u2", ch.id(), "u2");
    assertThat(b.calls).hasSize(2);
  }

  /** 3a minor M2: the join reads last_seq after the insert, so a message sequenced meanwhile is not left unread. */
  @Test
  void joinUsesTheLastSeqAfterTheInsert() {
    RecordingBadges b = new RecordingBadges();
    Conversations c = new Conversations(stores, stores, new MemberLookup(stores, stores), FakeStores.LIMITS,
        (user, conv) -> { }, UserDirectory.ANY, u -> ChatStores.Privacy.OPEN, b);
    Conversation g = c.create("u1", ConversationKind.CONVERSATION_KIND_GROUP, "g", List.of("u2"), false);
    stores.lastSeq.put(g.id(), 100L);
    stores.beforeAdd = () -> stores.lastSeq.put(g.id(), 101L);
    c.addMember("u1", g.id(), "u3");
    assertThat(b.calls).containsExactly("join u3 " + g.id() + " 101");
  }
}
