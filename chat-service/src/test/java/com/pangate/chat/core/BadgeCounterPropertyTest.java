package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.v1.ConversationKind;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Perf 3a (spec §4): whatever happens — new messages, reads, redelivery, joins, leaves, a lost hash, a raise on a
 * missing hash — the maintained badge equals the sum recomputed from the inbox. Every store write comes before its
 * counter update, as in the fan-out, the receipt worker and membership changes.
 */
class BadgeCounterPropertyTest {

  static final List<String> USERS = List.of("u1", "u2", "u3");

  @Test
  void totalAlwaysMatchesInbox() {
    for (int run = 0; run < 200; run++) {
      run(new Random(42L + run));
    }
  }

  record Sent(String conv, String sender, long seq) {}

  void run(Random rnd) {
    FakeStores s = new FakeStores();
    FakeBadges c = new FakeBadges();
    AtomicLong wall = new AtomicLong(1_000_000L);
    Badge b = new Badge(s, c, wall::get);
    List<String> convs = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      String id = Ids.next();
      ConversationKind k = i == 0 ? ConversationKind.CONVERSATION_KIND_CHANNEL
          : i == 1 ? ConversationKind.CONVERSATION_KIND_DIRECT : ConversationKind.CONVERSATION_KIND_GROUP;
      List<Member> ms = i == 1 ? List.of(new Member("u1", Role.MEMBER), new Member("u2", Role.MEMBER))
          : List.of(new Member("u1", Role.OWNER));
      s.create(new Conversation(id, k, "t", "u1", 0, false, ms.size()), ms);
      convs.add(id);
    }
    List<Sent> history = new ArrayList<>();
    for (int step = 0; step < 300; step++) {
      String conv = convs.get(rnd.nextInt(convs.size()));
      boolean channel = conv.equals(convs.get(0));
      Map<String, Member> ms = s.members.getOrDefault(conv, Map.of());
      String user = USERS.get(rnd.nextInt(USERS.size()));
      switch (rnd.nextInt(7)) {
        case 0, 1 -> {   // a new message
          if (!ms.isEmpty()) {
            String sender = new ArrayList<>(ms.keySet()).get(rnd.nextInt(ms.size()));
            long seq = s.lastSeq.getOrDefault(conv, 0L) + 1;
            s.lastSeq.put(conv, seq);
            deliver(s, b, new Sent(conv, sender, seq), channel);
            history.add(new Sent(conv, sender, seq));
          }
        }
        case 2 -> {   // a read mark
          if (ms.containsKey(user) && s.lastSeq.getOrDefault(conv, 0L) > 0) {
            long r = 1 + rnd.nextInt((int) (long) s.lastSeq.get(conv));
            s.advance(conv, user, r, r, 0, 1);
            if (!channel) {
              b.raise(user, conv, 0, r);
            }
          }
        }
        case 3 -> {   // Kafka hands an older event out again
          if (!history.isEmpty()) {
            Sent old = history.get(rnd.nextInt(history.size()));
            deliver(s, b, old, old.conv().equals(convs.get(0)));
          }
        }
        case 4 -> {   // join a group or the channel
          if (!ms.containsKey(user) && !conv.equals(convs.get(1))) {
            long last = s.lastSeq.getOrDefault(conv, 0L);
            s.add(conv, new Member(user, Role.MEMBER));
            if (!channel && last > 0) {
              b.raise(user, conv, last, last);
            }
          }
        }
        case 5 -> {   // leave
          if (ms.containsKey(user) && !conv.equals(convs.get(1))) {
            s.remove(conv, user);
            b.drop(user, conv);
          }
        }
        default -> c.lose(user);   // Redis lost the hash
      }
      for (String u : USERS) {
        assertThat(b.total(u, null, 0)).as("step %d user %s", step, u).isEqualTo(expected(s, u));
      }
    }
  }

  /** The fan-out of one message: inbox rows, then counters; the sender's marks, then theirs. */
  static void deliver(FakeStores s, Badge b, Sent m, boolean channel) {
    List<String> others = new ArrayList<>();
    if (!channel) {
      for (String u : s.members.getOrDefault(m.conv(), Map.of()).keySet()) {
        s.touchUserConv(u, m.conv(), m.seq(), m.seq());
        if (!u.equals(m.sender())) {
          others.add(u);
        }
      }
      b.raiseAll(others, m.conv(), m.seq());
    }
    if (s.members.getOrDefault(m.conv(), Map.of()).containsKey(m.sender())) {
      s.sent(m.conv(), m.sender(), m.seq(), m.seq());
      if (!channel) {
        b.raise(m.sender(), m.conv(), m.seq(), m.seq());
      }
    }
  }

  static int expected(FakeStores s, String user) {
    long sum = 0;
    for (ChatStores.UserConv u : s.userConvs(user, null, 10_000)) {
      if (u.kind() != ConversationKind.CONVERSATION_KIND_CHANNEL) {
        sum += Math.max(0, u.lastSeq() - u.readSeq());
      }
    }
    return (int) sum;
  }
}
