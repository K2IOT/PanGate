package com.pangate.chat.core;

import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.v1.ConversationKind;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Conversation management (chat spec §7): groups (at most {@code maxGroupMembers}) and channels with the creator as
 * owner, one DM per pair, members added or removed by an owner or admin (anyone may leave), public channels joined.
 * Permission checks read the store, not the cache; every membership change evicts the cached member list at once,
 * so a removed member gets nothing sent after the change.
 */
public final class Conversations {

  static final int MAX_TITLE = 256;
  static final int MAX_USER_ID = 128;

  private final ChatStores.ConversationStore conversations;
  private final ChatStores.MemberStore members;
  private final MemberLookup lookup;
  private final ChatProperties.Limits limits;

  public Conversations(ChatStores.ConversationStore conversations, ChatStores.MemberStore members,
      MemberLookup lookup, ChatProperties.Limits limits) {
    this.conversations = conversations;
    this.members = members;
    this.lookup = lookup;
    this.limits = limits;
  }

  static void checkUserId(String id) {
    if (id == null || id.isBlank() || id.length() > MAX_USER_ID) {
      throw ChatRefusal.invalid("user ids are 1.." + MAX_USER_ID + " characters");
    }
  }

  public Conversation create(String userId, ConversationKind kind, String title, List<String> memberIds,
      boolean isPublic) {
    if (kind != ConversationKind.CONVERSATION_KIND_GROUP && kind != ConversationKind.CONVERSATION_KIND_CHANNEL) {
      throw ChatRefusal.invalid("kind must be group or channel");
    }
    String t = title == null ? "" : title;
    if (t.length() > MAX_TITLE) {
      throw ChatRefusal.invalid("title is at most " + MAX_TITLE + " characters");
    }
    Set<String> others = new LinkedHashSet<>();
    for (String m : memberIds == null ? List.<String>of() : memberIds) {
      checkUserId(m);
      if (!m.equals(userId)) {
        others.add(m);
      }
    }
    if (kind == ConversationKind.CONVERSATION_KIND_GROUP && others.size() + 1 > limits.maxGroupMembers()) {
      throw ChatRefusal.invalid("a group has at most " + limits.maxGroupMembers() + " members");
    }
    List<Member> ms = new ArrayList<>();
    ms.add(new Member(userId, Role.OWNER));
    others.forEach(m -> ms.add(new Member(m, Role.MEMBER)));
    Conversation c = new Conversation(Ids.next(), kind, t, userId, 0,
        kind == ConversationKind.CONVERSATION_KIND_CHANNEL && isPublic, ms.size());
    conversations.create(c, ms);
    return c;
  }

  /** The pair's DM, created on first use; a dm_index entry left without its conversation is completed. */
  public Conversation openDm(String userId, String otherId) {
    checkUserId(otherId);
    if (otherId.equals(userId)) {
      throw ChatRefusal.invalid("a DM needs another user");
    }
    String id = conversations.dm(userId, otherId).orElseGet(() -> conversations.createDm(userId, otherId,
        Ids.next()));
    Optional<Conversation> c = conversations.get(id);
    if (c.isPresent()) {
      return c.get();
    }
    Conversation dm = new Conversation(id, ConversationKind.CONVERSATION_KIND_DIRECT, "", userId, 0, false, 2);
    conversations.create(dm, List.of(new Member(userId, Role.MEMBER), new Member(otherId, Role.MEMBER)));
    return dm;
  }

  /** The conversation if {@code userId} may read it: a member, or anyone for a public channel. */
  public Conversation visible(String userId, String convId) {
    Conversation c = conversations.get(convId).orElseThrow(ChatRefusal::notFound);
    if (c.kind() == ConversationKind.CONVERSATION_KIND_CHANNEL && c.isPublic()) {
      return c;
    }
    if (members.member(convId, userId).isEmpty()) {
      throw ChatRefusal.forbidden("not a member");
    }
    return c;
  }

  private Conversation managed(String actor, String convId) {
    Conversation c = conversations.get(convId).orElseThrow(ChatRefusal::notFound);
    if (c.kind() == ConversationKind.CONVERSATION_KIND_DIRECT) {
      throw ChatRefusal.invalid("a DM's members do not change");
    }
    return c;
  }

  private Role roleOf(String convId, String userId) {
    return members.member(convId, userId).map(Member::role).orElse(null);
  }

  public void addMember(String actor, String convId, String userId) {
    checkUserId(userId);
    Conversation c = managed(actor, convId);
    Role r = roleOf(convId, actor);
    if (r != Role.OWNER && r != Role.ADMIN) {
      throw ChatRefusal.forbidden("only an owner or admin adds members");
    }
    if (members.member(convId, userId).isPresent()) {
      return;
    }
    if (c.kind() == ConversationKind.CONVERSATION_KIND_GROUP
        && members.members(convId).size() + 1 > limits.maxGroupMembers()) {
      throw ChatRefusal.invalid("a group has at most " + limits.maxGroupMembers() + " members");
    }
    members.add(convId, new Member(userId, Role.MEMBER));
    lookup.evict(convId);
  }

  public void removeMember(String actor, String convId, String userId) {
    checkUserId(userId);
    managed(actor, convId);
    if (!actor.equals(userId)) {
      Role r = roleOf(convId, actor);
      Role target = roleOf(convId, userId);
      if (r != Role.OWNER && r != Role.ADMIN) {
        throw ChatRefusal.forbidden("only an owner or admin removes members");
      }
      if (target == Role.OWNER || (target == Role.ADMIN && r != Role.OWNER)) {
        throw ChatRefusal.forbidden("only the owner removes an admin; nobody removes the owner");
      }
    }
    members.remove(convId, userId);
    lookup.evict(convId);
  }

  /** Follows a public channel. */
  public void join(String userId, String convId) {
    Conversation c = conversations.get(convId).orElseThrow(ChatRefusal::notFound);
    if (c.kind() != ConversationKind.CONVERSATION_KIND_CHANNEL || !c.isPublic()) {
      throw ChatRefusal.forbidden("only public channels can be joined");
    }
    if (members.member(convId, userId).isEmpty()) {
      members.add(convId, new Member(userId, Role.MEMBER));
      lookup.evict(convId);
    }
  }
}
