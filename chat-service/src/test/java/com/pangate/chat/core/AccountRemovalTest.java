package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.v1.ConversationKind;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** account spec §8: a deleted account leaves every group and channel; its DMs stay. */
class AccountRemovalTest {

  final FakeStores stores = new FakeStores();
  final List<String> unsubscribed = new ArrayList<>();
  final Conversations convs = new Conversations(stores, stores, new MemberLookup(stores, stores), FakeStores.LIMITS,
      (user, conv) -> unsubscribed.add(user + "@" + conv));
  final AccountRemoval removal = new AccountRemoval(stores, convs);

  String conv(ConversationKind kind, Member... ms) {
    String id = Ids.next();
    stores.create(new Conversation(id, kind, "t", ms[0].userId(), 0, false, ms.length), List.of(ms));
    return id;
  }

  @Test
  void ownerPassesToTheEarliestAdminThenMember() {
    String withAdmin = conv(ConversationKind.CONVERSATION_KIND_GROUP, new Member("gone", Role.OWNER),
        new Member("m1", Role.MEMBER), new Member("a1", Role.ADMIN), new Member("a2", Role.ADMIN));
    String noAdmin = conv(ConversationKind.CONVERSATION_KIND_CHANNEL, new Member("gone", Role.OWNER),
        new Member("m1", Role.MEMBER), new Member("m2", Role.MEMBER));
    String alone = conv(ConversationKind.CONVERSATION_KIND_GROUP, new Member("gone", Role.OWNER));
    removal.removed("gone");
    assertThat(stores.member(withAdmin, "gone")).isEmpty();
    assertThat(stores.member(withAdmin, "a1")).get().extracting(Member::role).isEqualTo(Role.OWNER);
    assertThat(stores.member(withAdmin, "a2")).get().extracting(Member::role).isEqualTo(Role.ADMIN);
    assertThat(stores.member(noAdmin, "m1")).get().extracting(Member::role).isEqualTo(Role.OWNER);
    assertThat(stores.member(alone, "gone")).isEmpty();
    assertThat(unsubscribed).containsExactly("gone@" + noAdmin);
  }

  @Test
  void dmIsKept() {
    String dm = conv(ConversationKind.CONVERSATION_KIND_DIRECT, new Member("gone", Role.MEMBER),
        new Member("u2", Role.MEMBER));
    removal.removed("gone");
    assertThat(stores.member(dm, "gone")).isPresent();
  }

  @Test
  void runningTwiceChangesNothing() {
    String g = conv(ConversationKind.CONVERSATION_KIND_GROUP, new Member("gone", Role.OWNER),
        new Member("m1", Role.MEMBER), new Member("m2", Role.MEMBER));
    removal.removed("gone");
    removal.removed("gone");
    assertThat(stores.members(g)).extracting(Member::userId).containsExactlyInAnyOrder("m1", "m2");
    assertThat(stores.member(g, "m1")).get().extracting(Member::role).isEqualTo(Role.OWNER);
    assertThat(stores.member(g, "m2")).get().extracting(Member::role).isEqualTo(Role.MEMBER);
  }
}
