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
 * Permission checks read the store, not the cache; every membership change evicts the cached member list at once
 * (and fails with 503 if it cannot), so a removed member gets nothing sent after the change; leaving a channel also
 * ends its subscriptions.
 */
public final class Conversations {

  static final int MAX_TITLE = 256;
  /** Review 2026-11-19 (F7): others put in by one create. */
  static final int MAX_CREATE_MEMBERS = 200;

  /** A created group or channel and who was left out because their whoCanAdd is nobody. */
  public record Created(Conversation conversation, List<String> notAdded) {}
  static final int MAX_USER_ID = 128;

  private final ChatStores.ConversationStore conversations;
  private final ChatStores.MemberStore members;
  private final MemberLookup lookup;
  private final ChatProperties.Limits limits;
  private final java.util.function.BiConsumer<String, String> leftChannel;
  private final UserDirectory users;
  private final java.util.function.Function<String, ChatStores.Privacy> privacy;

  public Conversations(ChatStores.ConversationStore conversations, ChatStores.MemberStore members,
      MemberLookup lookup, ChatProperties.Limits limits) {
    this(conversations, members, lookup, limits, (user, conv) -> { });
  }

  /** {@code leftChannel(userId, convId)} after someone leaves or is removed from a channel. */
  public Conversations(ChatStores.ConversationStore conversations, ChatStores.MemberStore members,
      MemberLookup lookup, ChatProperties.Limits limits, java.util.function.BiConsumer<String, String> leftChannel) {
    this(conversations, members, lookup, limits, leftChannel, UserDirectory.ANY);
  }

  /** {@code users}: who may be put into a conversation (account spec §8: known and not deleted). */
  public Conversations(ChatStores.ConversationStore conversations, ChatStores.MemberStore members,
      MemberLookup lookup, ChatProperties.Limits limits, java.util.function.BiConsumer<String, String> leftChannel,
      UserDirectory users) {
    this(conversations, members, lookup, limits, leftChannel, users, u -> ChatStores.Privacy.OPEN);
  }

  /** {@code privacy}: each user's settings read from the store, not a cache (F7: whoCanAdd). */
  public Conversations(ChatStores.ConversationStore conversations, ChatStores.MemberStore members,
      MemberLookup lookup, ChatProperties.Limits limits, java.util.function.BiConsumer<String, String> leftChannel,
      UserDirectory users, java.util.function.Function<String, ChatStores.Privacy> privacy) {
    this.privacy = privacy;
    this.users = users;
    this.conversations = conversations;
    this.members = members;
    this.lookup = lookup;
    this.limits = limits;
    this.leftChannel = leftChannel;
  }

  static void checkUserId(String id) {
    if (id == null || id.isBlank() || id.length() > MAX_USER_ID) {
      throw ChatRefusal.invalid("user ids are 1.." + MAX_USER_ID + " characters");
    }
  }

  public Conversation create(String userId, ConversationKind kind, String title, List<String> memberIds,
      boolean isPublic) {
    return createReporting(userId, kind, title, memberIds, isPublic).conversation();
  }

  /** Review 2026-11-19 (F7): at most 200 others; those whose whoCanAdd is nobody are left out and reported. */
  public Created createReporting(String userId, ConversationKind kind, String title, List<String> memberIds,
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
    if (others.size() > MAX_CREATE_MEMBERS) {
      throw ChatRefusal.invalid("at most " + MAX_CREATE_MEMBERS + " members are added when creating");
    }
    if (kind == ConversationKind.CONVERSATION_KIND_GROUP && others.size() + 1 > limits.maxGroupMembers()) {
      throw ChatRefusal.invalid("a group has at most " + limits.maxGroupMembers() + " members");
    }
    others.forEach(users::requireActive);
    List<String> notAdded = others.stream().filter(m -> privacy.apply(m).refuseAdds()).toList();
    notAdded.forEach(others::remove);
    List<Member> ms = new ArrayList<>();
    ms.add(new Member(userId, Role.OWNER));
    others.forEach(m -> ms.add(new Member(m, Role.MEMBER)));
    Conversation c = new Conversation(Ids.next(), kind, t, userId, 0,
        kind == ConversationKind.CONVERSATION_KIND_CHANNEL && isPublic, ms.size());
    conversations.create(c, ms);
    return new Created(c, notAdded);
  }

  /** The pair's DM, created on first use; a dm_index entry left without its conversation is completed. */
  public Conversation openDm(String userId, String otherId) {
    checkUserId(otherId);
    if (otherId.equals(userId)) {
      throw ChatRefusal.invalid("a DM needs another user");
    }
    users.requireActive(otherId);
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

  /**
   * Batch 1 review I1: {@code owner_id} decides who owns (its member row may lag behind a transfer); a member row
   * still saying OWNER for someone else is an admin. A conversation without {@code owner_id} (created before V8)
   * keeps the member rows' word.
   */
  private Role roleOf(String convId, String userId) {
    Optional<Member> m = members.member(convId, userId);
    if (m.isEmpty()) {
      return null;
    }
    Optional<String> owner = conversations.owner(convId);
    if (owner.isEmpty()) {
      return m.get().role();
    }
    if (owner.get().equals(userId)) {
      return Role.OWNER;
    }
    return m.get().role() == Role.OWNER ? Role.ADMIN : m.get().role();
  }

  /** The owner as {@code owner_id} says, else (rows before V8) the member row saying OWNER. */
  private Optional<String> ownerOf(String convId) {
    return conversations.owner(convId).or(() -> members.membersByJoin(convId).stream()
        .filter(m -> m.role() == Role.OWNER).map(Member::userId).findFirst());
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
    users.requireActive(userId);
    if (privacy.apply(userId).refuseAdds()) {
      throw new ChatRefusal(ChatRefusal.ADD_REFUSED, "this user does not let others add them");
    }
    if (c.kind() == ConversationKind.CONVERSATION_KIND_GROUP
        && members.members(convId).size() + 1 > limits.maxGroupMembers()) {
      throw ChatRefusal.invalid("a group has at most " + limits.maxGroupMembers() + " members");
    }
    changeMembers(convId, () -> members.add(convId, new Member(userId, Role.MEMBER)));
  }

  /**
   * Drops the cached member list before the change and again after it. The first eviction also blocks refills for a
   * moment, so a list read before the change cannot be cached after it; and when Redis fails there, nothing is applied
   * (503, the caller retries) instead of a change that a surviving cached list would hide.
   */
  private void changeMembers(String convId, Runnable change) {
    evict(convId);
    change.run();
    evict(convId);
  }

  private void evict(String convId) {
    if (!lookup.evict(convId)) {
      throw new StoreUnavailableException("member cache of " + convId + " not evicted; retry", null);
    }
  }

  public void removeMember(String actor, String convId, String userId) {
    checkUserId(userId);
    Conversation c = managed(actor, convId);
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
    if (actor.equals(userId) && ownerOf(convId).map(userId::equals).orElse(false)) {
      handOverOwnership(convId, userId);
    }
    changeMembers(convId, () -> members.remove(convId, userId));
    if (c.kind() == ConversationKind.CONVERSATION_KIND_CHANNEL) {
      leftChannel.accept(userId, convId);   // its open subscriptions end now, not at the next reconnect
    }
  }

  /**
   * account spec §8: a deleted account leaves a group or channel as if it left itself (which hands ownership on, see
   * {@link #removeMember}). Not a member (any more): nothing happens.
   */
  public void leaveDeletedAccount(String userId, String convId) {
    if (members.member(convId, userId).isEmpty()) {
      return;
    }
    removeMember(userId, convId, userId);
  }

  /**
   * Review 2026-11-19 (F2), batch 1 review I1: ownership leaves {@code from} for the earliest admin, else the earliest
   * member. Each step is an LWT on {@code owner_id} that expects the current owner, so a racing transfer or leave wins
   * or loses as a whole: never two owners, never none. A successor that left meanwhile hands it on again.
   */
  private void handOverOwnership(String convId, String from) {
    Optional<String> stored = conversations.owner(convId);
    String expected = stored.orElse(null);
    if (stored.isPresent() && !stored.get().equals(from)) {
      return;   // someone else owns it already (a transfer won, or a retry after the hand-over)
    }
    if (stored.isEmpty()) {
      // before V8: another member row saying OWNER is a promotion that already happened; record it, nothing more
      Optional<Member> promoted = members.membersByJoin(convId).stream()
          .filter(m -> m.role() == Role.OWNER && !m.userId().equals(from)).findFirst();
      if (promoted.isPresent()) {
        conversations.claimOwner(convId, null, promoted.get().userId());
        return;
      }
    }
    String leaving = from;
    for (int attempt = 0; attempt < 5; attempt++) {
      String gone = leaving;
      List<Member> rest = members.membersByJoin(convId).stream().filter(m -> !m.userId().equals(gone)).toList();
      Optional<Member> next = rest.stream().filter(m -> m.role() == Role.ADMIN).findFirst()
          .or(() -> rest.stream().findFirst());
      if (next.isEmpty()) {
        return;   // nobody left to own it
      }
      if (!conversations.claimOwner(convId, expected, next.get().userId())) {
        return;   // the owner changed meanwhile: not ours to hand over
      }
      changeMembers(convId, () -> members.setRole(convId, next.get().userId(), Role.OWNER));
      if (members.member(convId, next.get().userId()).isPresent()) {
        return;
      }
      expected = next.get().userId();   // they left meanwhile: hand it on from them
      leaving = expected;
    }
  }

  /** Review 2026-11-19 (F2): the owner makes a member admin or an admin member again. */
  public void setRole(String actor, String convId, String userId, Role role) {
    checkUserId(userId);
    managed(actor, convId);
    if (roleOf(convId, actor) != Role.OWNER) {
      throw ChatRefusal.forbidden("only the owner sets roles");
    }
    if (role == Role.OWNER) {
      throw ChatRefusal.invalid("ownership moves with a transfer");
    }
    Role current = roleOf(convId, userId);
    if (current == null) {
      throw new ChatRefusal(ChatRefusal.NOT_FOUND, "not a member");
    }
    if (current == Role.OWNER) {
      throw ChatRefusal.invalid("the owner's role moves with a transfer");
    }
    if (current != role) {
      changeMembers(convId, () -> members.setRole(convId, userId, role));
    }
  }

  /** Review 2026-11-19 (F2): the owner hands ownership to a member and stays on as an admin. */
  public void transferOwnership(String actor, String convId, String userId) {
    checkUserId(userId);
    managed(actor, convId);
    if (roleOf(convId, actor) != Role.OWNER) {
      throw ChatRefusal.forbidden("only the owner transfers ownership");
    }
    if (actor.equals(userId)) {
      throw ChatRefusal.invalid("already the owner");
    }
    if (roleOf(convId, userId) == null) {
      throw new ChatRefusal(ChatRefusal.NOT_FOUND, "not a member");
    }
    // batch 1 review I1: the LWT decides; a racing transfer or leave that moved owner_id first makes this one lose
    if (!conversations.claimOwner(convId, conversations.owner(convId).orElse(null), userId)) {
      throw ChatRefusal.forbidden("ownership changed meanwhile");
    }
    changeMembers(convId, () -> members.setRole(convId, userId, Role.OWNER));
    if (members.member(convId, userId).isEmpty()) {
      handOverOwnership(convId, userId);   // they left meanwhile: it moves on, possibly back to the actor
    }
    if (!ownerOf(convId).map(actor::equals).orElse(false)) {
      changeMembers(convId, () -> members.setRole(convId, actor, Role.ADMIN));
    }
  }

  /** Follows a public channel. */
  public void join(String userId, String convId) {
    Conversation c = conversations.get(convId).orElseThrow(ChatRefusal::notFound);
    if (c.kind() != ConversationKind.CONVERSATION_KIND_CHANNEL || !c.isPublic()) {
      throw ChatRefusal.forbidden("only public channels can be joined");
    }
    if (members.member(convId, userId).isEmpty()) {
      changeMembers(convId, () -> members.add(convId, new Member(userId, Role.MEMBER)));
    }
  }
}
