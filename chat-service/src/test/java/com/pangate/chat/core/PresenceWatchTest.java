package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PresenceUpdate;
import com.pangate.contracts.v1.PresenceWatch;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** CH3 spec §6.3: who may watch whom, the snapshot, and change fan-out. */
class PresenceWatchTest {

  final FakeStores stores = new FakeStores();
  final FakePresence presence = new FakePresence();
  final AtomicLong now = new AtomicLong(1_800_000_000_000L);
  final FakeWatchers watchers = new FakeWatchers(now::get);
  final List<PushRequest> pushed = new CopyOnWriteArrayList<>();
  final PrivacyCache privacy = new PrivacyCache(stores, () -> 0L);
  final List<Pusher.Priority> priorities = new CopyOnWriteArrayList<>();
  final Pusher pusher = (r, p) -> {
    pushed.add(r);
    priorities.add(p);
    return CompletableFuture.completedFuture(PushResponse.getDefaultInstance());
  };
  final com.pangate.chat.core.PresenceWatch watch = new com.pangate.chat.core.PresenceWatch(stores,
      new MemberLookup(stores, stores), stores, watchers, presence, stores, privacy, pusher, now::get, Duration.ofMinutes(5),
      200);
  final PresenceFanOut fanOut = new PresenceFanOut(watchers, new MemberLookup(stores, stores), presence, stores, privacy,
      pusher, now::get);
  final String g = conv(ConversationKind.CONVERSATION_KIND_GROUP, "me", "a", "b");

  {
    stores.sent(g, "a", 1, 0);   // the presence rule: a and b took part in g
    stores.sent(g, "b", 2, 0);
  }

  String conv(ConversationKind kind, String... users) {
    String id = Ids.next();
    stores.create(new Conversation(id, kind, "t", users[0], 0, false, users.length),
        java.util.Arrays.stream(users).map(u -> new Member(u, Role.MEMBER)).toList());
    return id;
  }

  static PresenceWatch of(String conv, String... users) {
    PresenceWatch.Builder b = PresenceWatch.newBuilder();
    for (String u : users) {
      b.addEntries(PresenceWatch.Entry.newBuilder().setUserId(u).setConversationId(conv));
    }
    return b.build();
  }

  static List<String> users(PushRequest r) throws Exception {
    return PresenceUpdate.parseFrom(r.getPayload()).getStatesList().stream().map(PresenceUpdate.State::getUserId)
        .toList();
  }

  @Test
  void snapshotGoesToTheWatchingDeviceOnly() throws Exception {
    presence.add("a", "n1", 1, "c1");
    watch.watch("me", "d1", of(g, "a", "b"));
    assertThat(pushed).singleElement().satisfies(r -> {
      assertThat(r.getPrincipalId()).isEqualTo("chat:me");
      assertThat(r.getDeviceId()).isEqualTo("d1");
      assertThat(r.getHeadersMap()).containsEntry("type", ChatTopics.TYPE_PRESENCE);
      assertThat(r.getTtlSec()).isEqualTo(30);
    });
    PresenceUpdate u = PresenceUpdate.parseFrom(pushed.getFirst().getPayload());
    assertThat(u.getStatesList()).extracting(PresenceUpdate.State::getUserId, PresenceUpdate.State::getOnline)
        .containsExactlyInAnyOrder(org.assertj.core.groups.Tuple.tuple("a", true),
            org.assertj.core.groups.Tuple.tuple("b", false));
  }

  @Test
  void strangerEntryIsDroppedSilently() throws Exception {
    String other = conv(ConversationKind.CONVERSATION_KIND_GROUP, "x", "y");
    watch.watch("me", "d1", PresenceWatch.newBuilder().addAllEntries(of(g, "a").getEntriesList())
        .addAllEntries(of(other, "y").getEntriesList()).addAllEntries(of(g, "y").getEntriesList()).build());
    assertThat(users(pushed.getFirst())).containsExactly("a");
    assertThat(watchers.watchersOf("y", now.get())).isEmpty();
  }

  @Test
  void channelEntryIsDropped() {
    String ch = conv(ConversationKind.CONVERSATION_KIND_CHANNEL, "me", "a");
    watch.watch("me", "d1", of(ch, "a"));
    assertThat(pushed).isEmpty();
    assertThat(watchers.watchersOf("a", now.get())).isEmpty();
  }

  @Test
  void secondWatchReplacesTheFirst() {
    watch.watch("me", "d1", of(g, "a"));
    watch.watch("me", "d1", of(g, "b"));
    assertThat(watchers.watchersOf("a", now.get())).isEmpty();
    assertThat(watchers.watchersOf("b", now.get())).containsExactly("me/d1/" + g);
  }

  @Test
  void moreThan200KeepsFirst200() {
    String[] many = new String[210];
    Member[] ms = new Member[211];
    ms[0] = new Member("me", Role.MEMBER);
    for (int i = 0; i < 210; i++) {
      many[i] = "m" + i;
      ms[i + 1] = new Member(many[i], Role.MEMBER);
    }
    String big = Ids.next();
    stores.create(new Conversation(big, ConversationKind.CONVERSATION_KIND_GROUP, "b", "me", 0, false, 211),
        List.of(ms));
    for (int i = 0; i < 210; i++) {
      stores.sent(big, many[i], i + 1, 0);
    }
    watch.watch("me", "d1", of(big, many));
    assertThat(watchers.byDevice.get("me/d1").entries()).hasSize(200);
    assertThat(watchers.watchersOf("m199", now.get())).hasSize(1);
    assertThat(watchers.watchersOf("m200", now.get())).isEmpty();
  }

  /** Review Focus 2. */
  @Test
  void leftMemberIsNotWatchedAfterRefresh() {
    watch.watch("me", "d1", of(g, "a"));
    stores.remove(g, "a");
    stores.evictMembers(g);
    watch.watch("me", "d1", of(g, "a"));
    assertThat(watchers.watchersOf("a", now.get())).isEmpty();
  }

  @Test
  void hidePresenceViewerStillStoresWatchForTyping() throws Exception {
    stores.putPrivacy("me", new ChatStores.Privacy(true, false), 0);
    presence.add("a", "n1", 1, "c1");
    watch.watch("me", "d1", of(g, "a"));
    assertThat(watchers.devicesWithOpen(g)).containsExactly("me/d1");
    assertThat(PresenceUpdate.parseFrom(pushed.getFirst().getPayload()).getStates(0).getHidden()).isTrue();
  }

  @Test
  void watchExpiresAfterTtl() {
    watch.watch("me", "d1", of(g, "a"));
    now.addAndGet(Duration.ofMinutes(5).toMillis() + 1);
    assertThat(watchers.watchersOf("a", now.get())).isEmpty();
  }

  @Test
  void transitionReachesWatchersFiltered() throws Exception {
    watch.watch("me", "d1", of(g, "a"));
    watch.watch("b", "d9", of(g, "a"));
    stores.putPrivacy("b", new ChatStores.Privacy(true, false), 0);
    privacy.forget("b");   // as PUT /v1/me/privacy does
    pushed.clear();
    fanOut.on(new PresenceTracker.Transition("a", true, now.get()));
    // final review C2: a viewer who hides presence gets no push timed to others' transitions
    assertThat(pushed).singleElement().satisfies(r -> {
      assertThat(r.getPrincipalId()).isEqualTo("chat:me");
      assertThat(r.getDeviceId()).isEqualTo("d1");
    });
    assertThat(PresenceUpdate.parseFrom(pushed.getFirst().getPayload()).getStates(0).getOnline()).isTrue();
  }

  /** Final review C2: the transitions of a user who hides presence are pushed to nobody (no timing signal). */
  @Test
  void hiddenSubjectTransitionsArePushedToNobody() {
    watch.watch("me", "d1", of(g, "a"));
    stores.putPrivacy("a", new ChatStores.Privacy(true, false), 0);
    privacy.forget("a");
    pushed.clear();
    fanOut.on(new PresenceTracker.Transition("a", true, now.get()));
    fanOut.on(new PresenceTracker.Transition("a", false, now.get()));
    assertThat(pushed).isEmpty();
  }

  @Test
  void offlineTransitionCarriesLastSeen() throws Exception {
    watch.watch("me", "d1", of(g, "a"));
    pushed.clear();
    long at = now.get() - 61_000;
    fanOut.on(new PresenceTracker.Transition("a", false, at));
    PresenceUpdate.State s = PresenceUpdate.parseFrom(pushed.getFirst().getPayload()).getStates(0);
    assertThat(s.getOnline()).isFalse();
    assertThat(s.getLastSeenMs()).isEqualTo(at / 60_000 * 60_000);
  }

  /** Owner decision (2026-11-17): a DM counts only once the watched user has sent a message in it. */
  @Test
  void emptyDmDoesNotCount() {
    String dm = conv(ConversationKind.CONVERSATION_KIND_DIRECT, "me", "x1");
    watch.watch("me", "d1", of(dm, "x1"));
    assertThat(watchers.watchersOf("x1", now.get())).isEmpty();
    assertThat(pushed).isEmpty();
    stores.sent(dm, "me", 1, 0);   // the viewer's own message does not count
    watch.watch("me", "d1", of(dm, "x1"));
    assertThat(watchers.watchersOf("x1", now.get())).isEmpty();
    stores.sent(dm, "x1", 2, 0);
    watch.watch("me", "d1", of(dm, "x1"));
    assertThat(watchers.watchersOf("x1", now.get())).containsExactly("me/d1/" + dm);
  }

  /**
   * Owner decision (2026-11-17, after review of PR #6): a group counts like a DM, only once the watched user sent a
   * message there. Who added whom proves nothing: a second account can create the group.
   */
  @Test
  void groupCountsOnlyOnceTheWatchedUserTalked() {
    String theirs = conv(ConversationKind.CONVERSATION_KIND_GROUP, "x3", "me", "x4");   // x3, maybe a puppet, made it
    watch.watch("me", "d2", of(theirs, "x4", "x3"));
    assertThat(watchers.watchersOf("x4", now.get())).isEmpty();
    assertThat(watchers.watchersOf("x3", now.get())).isEmpty();
    assertThat(watch.states("me", theirs, List.of("x4", "x3"))).isEmpty();   // HTTP too
    stores.sent(theirs, "me", 1, 0);   // the viewer's own message does not count
    stores.sent(theirs, "x4", 2, 0);
    watch.watch("me", "d2", of(theirs, "x4", "x3"));
    assertThat(watchers.watchersOf("x4", now.get())).hasSize(1);
    assertThat(watchers.watchersOf("x3", now.get())).isEmpty();
  }

  /** Codex review of PR #6: a device that opened a group no one spoke in yet still counts as open for typing. */
  @Test
  void openConversationIsKnownWithoutPresenceEntries() {
    String quiet = conv(ConversationKind.CONVERSATION_KIND_GROUP, "me", "z1", "z2");
    watch.watch("me", "d1", of(quiet, "z1", "z2"));
    assertThat(watchers.watchersOf("z1", now.get())).isEmpty();
    assertThat(watchers.devicesWithOpen(quiet)).containsExactly("me/d1");
    String foreign = conv(ConversationKind.CONVERSATION_KIND_GROUP, "z3", "z4");
    watch.watch("me", "d1", of(foreign, "z3"));
    assertThat(watchers.devicesWithOpen(foreign)).isEmpty();   // not a member
    assertThat(watchers.devicesWithOpen(quiet)).isEmpty();     // the list was replaced
  }

  /** Codex review of PR #6: once the shared conversation no longer holds both, the watch tells nothing. */
  @Test
  void fanOutSkipsWatchesWhoseConversationNoLongerHoldsBoth() {
    watch.watch("me", "d1", of(g, "a", "b"));
    pushed.clear();
    stores.remove(g, "me");
    stores.evictMembers(g);
    assertThat(fanOut.on(new PresenceTracker.Transition("a", true, now.get()))).isZero();
    assertThat(pushed).isEmpty();
    stores.add(g, new Member("me", Role.MEMBER));
    stores.remove(g, "b");
    stores.evictMembers(g);
    assertThat(fanOut.on(new PresenceTracker.Transition("b", true, now.get()))).isZero();
    assertThat(fanOut.on(new PresenceTracker.Transition("a", true, now.get()))).isEqualTo(1);
  }

  /** Perf 3a (spec §3.1, §4): a watch of 200 entries in 10 conversations looks each conversation up once. */
  @Test
  void watchLooksUpEachConversationOnce() {
    PresenceWatch.Builder b = PresenceWatch.newBuilder();
    for (int c = 0; c < 10; c++) {
      String[] users = new String[21];
      users[0] = "me";
      for (int i = 0; i < 20; i++) {
        users[i + 1] = "p" + c + "_" + i;
      }
      String id = conv(ConversationKind.CONVERSATION_KIND_GROUP, users);
      for (int i = 0; i < 20; i++) {
        stores.sent(id, users[i + 1], i + 1, 0);
        b.addEntries(PresenceWatch.Entry.newBuilder().setUserId(users[i + 1]).setConversationId(id));
      }
    }
    stores.getCalls.set(0);
    stores.membersCalls.set(0);
    stores.cachedMembersCalls.set(0);
    assertThat(watch.watch("me", "d1", b.build())).isEqualTo(200);
    assertThat(stores.getCalls.get()).isLessThanOrEqualTo(10);
    // one list lookup per conversation (a cache miss reads the cache, then the store)
    assertThat(stores.cachedMembersCalls.get()).isLessThanOrEqualTo(10);
    assertThat(stores.membersCalls.get()).isLessThanOrEqualTo(10);
  }

  /** Perf 3b (spec §4.1): the watch snapshot and the transitions are droppable. */
  @Test
  void snapshotAndTransitionsAreDroppable() {
    presence.add("a", "n1", 1, "c1");
    watch.watch("me", "d1", of(g, "a"));
    fanOut.on(new PresenceTracker.Transition("a", false, now.get()));
    assertThat(pushed).isNotEmpty();
    assertThat(priorities).hasSameSizeAs(pushed).containsOnly(Pusher.Priority.DROPPABLE);
  }
}
