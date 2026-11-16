package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatRefusal;
import com.pangate.chat.core.FakeStores;
import com.pangate.contracts.v1.AccountState;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class AccountsListenerTest {

  final FakeStores stores = new FakeStores();
  final List<String> removed = new ArrayList<>();

  static ConsumerRecord<String, byte[]> rec(String user, AccountState.Status s, long at) {
    return new ConsumerRecord<>("account.users", 0, 0, user, AccountState.newBuilder().setUserId(user).setStatus(s)
        .setUpdatedAtMs(at).build().toByteArray());
  }

  /** Final review I3: once deleted, a later-stamped ACTIVE or LOCKED (a racing admin write) never revives it. */
  @Test
  void deletedIsSticky() {
    AccountsListener l = new AccountsListener(null, "account.users", stores, removed::add);
    l.handle(rec("u", AccountState.Status.STATUS_DELETED, 2));
    l.handle(rec("u", AccountState.Status.STATUS_LOCKED, 3));
    l.handle(rec("u", AccountState.Status.STATUS_ACTIVE, 4));
    assertThat(stores.user("u")).get().extracting(r -> r.status()).isEqualTo("deleted");
    assertThat(removed).containsExactly("u");
  }

  /** Final review I5: a refusal from the removal is deterministic: it must not hold the partition forever. */
  @Test
  void aRefusalDuringRemovalIsNotRetried() {
    AccountsListener l = new AccountsListener(null, "account.users", stores, u -> {
      throw new ChatRefusal(ChatRefusal.FORBIDDEN, "cannot");
    });
    l.handle(rec("u", AccountState.Status.STATUS_DELETED, 2));
    assertThat(stores.user("u")).get().extracting(r -> r.status()).isEqualTo("deleted");
  }
}
