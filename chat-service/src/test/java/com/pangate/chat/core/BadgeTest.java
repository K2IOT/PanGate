package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.UserConv;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

/** CH4 spec §3.1: the badge is every unread message, muted conversations included. */
class BadgeTest {

  @Test
  void sumOfUnreadNeverNegative() {
    FakeStores stores = new FakeStores();
    Map<String, UserConv> mine = new ConcurrentHashMap<>();
    mine.put("c1", new UserConv("c1", null, 10, 7, 1));
    mine.put("c2", new UserConv("c2", null, 5, 5, 2));
    mine.put("c3", new UserConv("c3", null, 3, 9, 3));      // read ahead of a stale last_seq: 0, not -6
    mine.put("c4", new UserConv("c4", null, 4, 0, 4, 0, Long.MAX_VALUE));   // muted still counts
    stores.userConvs.put("u", mine);
    assertThat(new Badge(stores).total("u")).isEqualTo(3 + 4);
    assertThat(new Badge(stores).total("nobody")).isZero();
  }
}
