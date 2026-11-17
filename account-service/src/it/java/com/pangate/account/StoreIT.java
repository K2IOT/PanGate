package com.pangate.account;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.account.application.port.AccountStore;
import com.pangate.account.application.port.Outbox;
import com.pangate.account.application.port.SessionStore;
import com.pangate.account.application.port.TakenException;
import com.pangate.account.application.port.Transactions;
import com.pangate.account.domain.Account;
import com.pangate.account.domain.AccountStatus;
import com.pangate.account.domain.Login;
import com.pangate.account.domain.Session;
import com.pangate.account.support.AccountIT;
import com.pangate.contracts.v1.AccountState;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@AccountIT
class StoreIT {

  @Autowired AccountStore accounts;
  @Autowired SessionStore sessions;
  @Autowired Outbox outbox;
  @Autowired Transactions tx;

  static String id() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 26).toUpperCase();
  }

  static String name() {
    return "u" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
  }

  static Account account(String username, String email, String phone) {
    return new Account(id(), username, username, null, "hash", false, AccountStatus.ACTIVE, email, phone,
        Instant.now(), null);
  }

  @Test
  void concurrentRegistrationsOfOneUsernameHaveOneWinner() throws Exception {
    String username = name();
    List<Future<String>> results = new ArrayList<>();
    try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int i = 0; i < 8; i++) {
        String email = name() + "@x.vn";
        results.add(pool.submit(() -> {
          try {
            tx.inTx(() -> accounts.insert(account(username, email, null)));
            return "ok";
          } catch (TakenException e) {
            return e.code();
          }
        }));
      }
    }
    List<String> codes = new ArrayList<>();
    for (Future<String> f : results) {
      codes.add(f.get());
    }
    assertThat(codes).containsOnlyOnce("ok");
    assertThat(codes.stream().filter("username_taken"::equals)).hasSize(7);
  }

  @Test
  void takenCodeNamesTheField() {
    String email = name() + "@x.vn";
    String phone = "+849" + (10_000_000 + (int) (Math.random() * 89_999_999));
    tx.inTx(() -> accounts.insert(account(name(), email, phone)));
    assertThatThrownBy(() -> tx.inTx(() -> accounts.insert(account(name(), email, null))))
        .isInstanceOfSatisfying(TakenException.class, e -> assertThat(e.code()).isEqualTo("email_taken"));
    assertThatThrownBy(() -> tx.inTx(() -> accounts.insert(account(name(), null, phone))))
        .isInstanceOfSatisfying(TakenException.class, e -> assertThat(e.code()).isEqualTo("phone_taken"));
  }

  @Test
  void byLoginFindsByEveryKind() {
    String username = "Lan" + name();
    String email = name() + "@x.vn";
    String phone = "+849" + (10_000_000 + (int) (Math.random() * 89_999_999));
    Account a = account(username, email, phone);
    tx.inTx(() -> accounts.insert(a));
    assertThat(accounts.byLogin(Login.parse(email.toUpperCase()))).get().extracting(Account::userId)
        .isEqualTo(a.userId());
    assertThat(accounts.byLogin(Login.parse(phone))).get().extracting(Account::userId).isEqualTo(a.userId());
    assertThat(accounts.byLogin(Login.parse(username.toUpperCase()))).get()
        .satisfies(x -> {
          assertThat(x.userId()).isEqualTo(a.userId());
          assertThat(x.username()).isEqualTo(username);
          assertThat(x.email()).isEqualTo(email);
          assertThat(x.phone()).isEqualTo(phone);
        });
    assertThat(accounts.byLogin(Login.parse("nobody" + name()))).isEmpty();
  }

  @Test
  void eraseFreesIdentifiersAndKeepsTheRow() {
    String username = name();
    String email = name() + "@x.vn";
    Account a = account(username, email, null);
    tx.inTx(() -> accounts.insert(a));
    tx.inTx(() -> accounts.erase(a.userId()));
    assertThat(accounts.byId(a.userId())).get().satisfies(x -> {
      assertThat(x.status()).isEqualTo(AccountStatus.DELETED);
      assertThat(x.username()).isNull();
      assertThat(x.displayName()).isNull();
      assertThat(x.passwordHash()).isNull();
      assertThat(x.email()).isNull();
      assertThat(x.deletedAt()).isNotNull();
    });
    tx.inTx(() -> accounts.insert(account(username, email, null)));
    assertThat(accounts.byLogin(Login.parse(email))).get().extracting(Account::userId).isNotEqualTo(a.userId());
  }

  @Test
  void revokeAllExceptKeepsTheCurrentDevice() {
    Account a = account(name(), name() + "@x.vn", null);
    tx.inTx(() -> accounts.insert(a));
    List<String> devices = List.of(id(), id(), id());
    for (String d : devices) {
      sessions.insert(new Session(d, a.userId(), "phone", "ios", Instant.now(), Instant.now(), null),
          d.getBytes());
    }
    assertThat(tx.inTx(() -> sessions.revokeAll(a.userId(), devices.get(1))))
        .containsExactlyInAnyOrder(devices.get(0), devices.get(2));
    assertThat(sessions.live(a.userId())).extracting(Session::deviceId).containsExactly(devices.get(1));
    assertThat(sessions.isLive(a.userId(), devices.get(0))).isFalse();
    assertThat(sessions.isLive(a.userId(), devices.get(1))).isTrue();
    assertThat(sessions.isLive("someone-else", devices.get(1))).as("the session must be the token's user's").isFalse();
    assertThat(sessions.byRefreshHash(devices.get(1).getBytes())).get().extracting(Session::revokedAt).isNull();
  }

  @Test
  void outboxKeepsOrderPerKind() {
    String u = id();
    tx.inTx(() -> {
      outbox.event(u, AccountState.Status.STATUS_ACTIVE);
      outbox.revokeDevice(u, "D1");
      outbox.event(u, AccountState.Status.STATUS_DELETED);
    });
    List<Outbox.OutboxRow> events = outbox.pending("event", 500).stream().filter(r -> r.key().equals(u)).toList();
    assertThat(events).hasSize(2);
    assertThat(events.get(0).id()).isLessThan(events.get(1).id());
    List<Outbox.OutboxRow> revs = outbox.pending("revocation", 500).stream().filter(r -> r.key().equals(u)).toList();
    assertThat(revs).singleElement().satisfies(r -> assertThat(new String(r.payload()))
        .isEqualTo("{\"kind\":\"device\",\"principal\":\"chat:" + u + "\",\"device\":\"D1\"}"));
  }
  /** Final review I3: a write that races with a deletion must not bring personal data or a status back. */
  @Test
  void writesToADeletedAccountAreRefused() {
    Account a = account(name(), name() + "@x.vn", null);
    tx.inTx(() -> accounts.insert(a));
    tx.inTx(() -> accounts.erase(a.userId()));
    for (Runnable w : List.<Runnable>of(
        () -> accounts.updateProfile(a.userId(), "Back", "again"),
        () -> accounts.updateUsername(a.userId(), com.pangate.account.domain.Username.of(name(), java.util.Set.of())),
        () -> accounts.setIdentifier(a.userId(), com.pangate.account.domain.LoginKind.EMAIL, name() + "@x.vn"),
        () -> accounts.setPassword(a.userId(), "h", false),
        () -> accounts.setStatus(a.userId(), AccountStatus.LOCKED))) {
      assertThatThrownBy(() -> tx.inTx(w)).isInstanceOfSatisfying(com.pangate.account.domain.DomainException.class,
          e -> assertThat(e.code()).isEqualTo("not_found"));
    }
    assertThat(accounts.byId(a.userId())).get().satisfies(x -> {
      assertThat(x.status()).isEqualTo(AccountStatus.DELETED);
      assertThat(x.displayName()).isNull();
      assertThat(x.username()).isNull();
      assertThat(x.email()).isNull();
      assertThat(x.passwordHash()).isNull();
    });
  }
  /** Final review minor: event times come from one clock, the database's, not from each pod. */
  @Test
  void eventTimesComeFromTheDatabaseClock() throws Exception {
    String u = id();
    long db = jdbc().sql("SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint").query(Long.class).single();
    tx.inTx(() -> outbox.event(u, AccountState.Status.STATUS_ACTIVE));
    byte[] p = outbox.pending("event", 10_000).stream().filter(r -> r.key().equals(u)).findFirst().get().payload();
    AccountState st = AccountState.parseFrom(p);
    assertThat(st.getUserId()).isEqualTo(u);
    assertThat(st.getUpdatedAtMs()).isBetween(db - 1_000, db + 5_000);
  }

  /** Final review minor: a deleted account's device names ("iPhone của Lan") are erased with it. */
  @Test
  void eraseAlsoErasesDeviceNames() {
    Account a = account(name(), name() + "@x.vn", null);
    tx.inTx(() -> accounts.insert(a));
    String d = id();
    sessions.insert(new Session(d, a.userId(), "iPhone của Lan", "ios", Instant.now(), Instant.now(), null),
        d.getBytes());
    tx.inTx(() -> accounts.erase(a.userId()));
    assertThat(sessions.byDeviceId(d)).get().extracting(Session::deviceName).isEqualTo("(deleted)");
  }

  @Autowired org.springframework.jdbc.core.simple.JdbcClient jdbcClient;

  org.springframework.jdbc.core.simple.JdbcClient jdbc() {
    return jdbcClient;
  }
}
