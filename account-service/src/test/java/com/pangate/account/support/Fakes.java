package com.pangate.account.support;

import com.pangate.account.application.port.AccountStore;
import com.pangate.account.application.port.AdminLog;
import com.pangate.account.application.port.Clock;
import com.pangate.account.application.port.Outbox;
import com.pangate.account.application.port.PasswordHasher;
import com.pangate.account.application.port.SessionStore;
import com.pangate.account.application.port.TakenException;
import com.pangate.account.application.port.Transactions;
import com.pangate.account.domain.Account;
import com.pangate.account.domain.AccountStatus;
import com.pangate.account.domain.Login;
import com.pangate.account.domain.LoginKind;
import com.pangate.account.domain.Session;
import com.pangate.account.domain.Username;
import com.pangate.contracts.v1.AccountState;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/** In-memory ports for use-case tests (no rollback: tests do not rely on it). */
public final class Fakes {

  public final Map<String, Account> accounts = new ConcurrentHashMap<>();
  public final Map<String, Session> sessions = new ConcurrentHashMap<>();
  public final Map<String, byte[]> refreshHashes = new ConcurrentHashMap<>();
  public final List<Outbox.OutboxRow> outboxRows = new ArrayList<>();
  public final List<String> adminActions = new ArrayList<>();
  /** For each admin_actions row: whether it was written inside a transaction. */
  public final List<Boolean> adminActionsInTx = new ArrayList<>();
  private volatile boolean inTx;
  public final AtomicInteger burns = new AtomicInteger();
  public final AtomicInteger touches = new AtomicInteger();
  private final AtomicLong outboxIds = new AtomicLong();
  public Instant now = Instant.parse("2026-11-16T10:00:00Z");

  public final Clock clock = () -> now;

  /** "h:" + password: fast, and still only matches the same NFC password. */
  public final PasswordHasher hasher = new PasswordHasher() {
    @Override
    public String hash(char[] pw) {
      return "h:" + new String(pw);
    }

    @Override
    public boolean matches(char[] pw, String hash) {
      return hash != null && hash.equals("h:" + new String(pw));
    }

    @Override
    public void burn() {
      burns.incrementAndGet();
    }
  };

  public final Transactions tx = new Transactions() {
    @Override
    public synchronized <T> T inTx(Supplier<T> work) {
      boolean outer = inTx;
      inTx = true;
      try {
        return work.get();
      } finally {
        inTx = outer;
      }
    }

    @Override
    public synchronized void inTx(Runnable work) {
      inTx(() -> {
        work.run();
        return null;
      });
    }
  };

  static String key(String username) {
    return username == null ? null : username.toLowerCase(Locale.ROOT);
  }

  public final AccountStore accountStore = new AccountStore() {
    @Override
    public synchronized void insert(Account a) {
      for (Account o : accounts.values()) {
        if (a.username() != null && Objects.equals(key(o.username()), key(a.username()))) {
          throw new TakenException("username_taken");
        }
        if (a.email() != null && a.email().equals(o.email())) {
          throw new TakenException("email_taken");
        }
        if (a.phone() != null && a.phone().equals(o.phone())) {
          throw new TakenException("phone_taken");
        }
      }
      accounts.put(a.userId(), a);
    }

    @Override
    public Optional<Account> byId(String userId) {
      return Optional.ofNullable(accounts.get(userId));
    }

    @Override
    public Optional<Account> byLogin(Login l) {
      return accounts.values().stream().filter(a -> switch (l.kind()) {
        case EMAIL -> l.key().equals(a.email());
        case PHONE -> l.key().equals(a.phone());
        case USERNAME -> l.key().equals(key(a.username()));
      }).findFirst();
    }

    @Override
    public Optional<Account> byUsernameKey(String k) {
      return accounts.values().stream().filter(a -> k.equals(key(a.username()))).findFirst();
    }

    @Override
    public List<Account> byIds(Collection<String> ids) {
      return ids.stream().map(accounts::get).filter(Objects::nonNull).toList();
    }

    @Override
    public void updateProfile(String userId, String displayName, String bio) {
      accounts.computeIfPresent(userId, (k, a) -> new Account(a.userId(), a.username(), displayName, bio,
          a.passwordHash(), a.mustChangePassword(), a.status(), a.email(), a.phone(), a.createdAt(), a.deletedAt()));
    }

    @Override
    public synchronized void updateUsername(String userId, Username u) {
      if (byUsernameKey(u.key()).filter(a -> !a.userId().equals(userId)).isPresent()) {
        throw new TakenException("username_taken");
      }
      accounts.computeIfPresent(userId, (k, a) -> new Account(a.userId(), u.display(), a.displayName(), a.bio(),
          a.passwordHash(), a.mustChangePassword(), a.status(), a.email(), a.phone(), a.createdAt(), a.deletedAt()));
    }

    @Override
    public synchronized void setIdentifier(String userId, LoginKind kind, String value) {
      if (value != null && accounts.values().stream().anyMatch(a -> !a.userId().equals(userId)
          && value.equals(kind == LoginKind.EMAIL ? a.email() : a.phone()))) {
        throw new TakenException(kind == LoginKind.EMAIL ? "email_taken" : "phone_taken");
      }
      accounts.computeIfPresent(userId, (k, a) -> new Account(a.userId(), a.username(), a.displayName(), a.bio(),
          a.passwordHash(), a.mustChangePassword(), a.status(), kind == LoginKind.EMAIL ? value : a.email(),
          kind == LoginKind.PHONE ? value : a.phone(), a.createdAt(), a.deletedAt()));
    }

    @Override
    public void setPassword(String userId, String hash, boolean mustChange) {
      accounts.computeIfPresent(userId, (k, a) -> new Account(a.userId(), a.username(), a.displayName(), a.bio(),
          hash, mustChange, a.status(), a.email(), a.phone(), a.createdAt(), a.deletedAt()));
    }

    @Override
    public void setStatus(String userId, AccountStatus s) {
      accounts.computeIfPresent(userId, (k, a) -> new Account(a.userId(), a.username(), a.displayName(), a.bio(),
          a.passwordHash(), a.mustChangePassword(), s, a.email(), a.phone(), a.createdAt(), a.deletedAt()));
    }

    @Override
    public void erase(String userId) {
      accounts.computeIfPresent(userId, (k, a) -> new Account(a.userId(), null, null, null, null, false,
          AccountStatus.DELETED, null, null, a.createdAt(), now));
    }
  };

  public final SessionStore sessionStore = new SessionStore() {
    @Override
    public void insert(Session s, byte[] h) {
      sessions.put(s.deviceId(), s);
      refreshHashes.put(s.deviceId(), h);
    }

    @Override
    public Optional<Session> byRefreshHash(byte[] h) {
      return refreshHashes.entrySet().stream().filter(e -> Arrays.equals(e.getValue(), h))
          .map(e -> sessions.get(e.getKey())).findFirst();
    }

    @Override
    public Optional<Session> byDeviceId(String d) {
      return Optional.ofNullable(sessions.get(d));
    }

    @Override
    public List<Session> live(String userId) {
      return sessions.values().stream().filter(s -> s.userId().equals(userId) && s.revokedAt() == null)
          .sorted(java.util.Comparator.comparing(Session::createdAt).thenComparing(Session::deviceId)).toList();
    }

    @Override
    public synchronized List<String> revokeAll(String userId, String except) {
      List<String> out = new ArrayList<>();
      for (Session s : live(userId)) {
        if (!s.deviceId().equals(except)) {
          revoke(userId, s.deviceId());
          out.add(s.deviceId());
        }
      }
      return out;
    }

    @Override
    public synchronized boolean revoke(String userId, String d) {
      Session s = sessions.get(d);
      if (s == null || !s.userId().equals(userId) || s.revokedAt() != null) {
        return false;
      }
      sessions.put(d, new Session(d, s.userId(), s.deviceName(), s.platform(), s.createdAt(), s.lastUsedAt(), now));
      return true;
    }

    @Override
    public void touch(String d, Instant at) {
      touches.incrementAndGet();
      sessions.computeIfPresent(d, (k, s) -> new Session(d, s.userId(), s.deviceName(), s.platform(), s.createdAt(),
          at, s.revokedAt()));
    }

    @Override
    public boolean isLive(String userId, String d) {
      Session s = sessions.get(d);
      return s != null && s.revokedAt() == null && s.userId().equals(userId);
    }

    @Override
    public int expireIdle(Instant before) {
      int n = 0;
      for (Session s : List.copyOf(sessions.values())) {
        if (s.revokedAt() == null && s.lastUsedAt().isBefore(before)) {
          n += revoke(s.userId(), s.deviceId()) ? 1 : 0;
        }
      }
      return n;
    }

    @Override
    public int purgeRevoked(Instant before) {
      return 0;
    }

    @Override
    public long countLive() {
      return sessions.values().stream().filter(s -> s.revokedAt() == null).count();
    }
  };

  public final Outbox outbox = new Outbox() {
    private synchronized void add(String kind, String key, byte[] payload) {
      outboxRows.add(new OutboxRow(outboxIds.incrementAndGet(), kind, key, payload));
    }

    @Override
    public void event(String userId, AccountState.Status s) {
      add(EVENT, userId, AccountState.newBuilder().setUserId(userId).setStatus(s).setUpdatedAtMs(now.toEpochMilli())
          .build().toByteArray());
    }

    @Override
    public void revokeDevice(String userId, String d) {
      add(REVOCATION, userId, ("device:" + d).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void revokePrincipal(String userId) {
      add(REVOCATION, userId, "principal".getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public synchronized List<OutboxRow> pending(String kind, int limit) {
      return outboxRows.stream().filter(r -> r.kind().equals(kind)).limit(limit).toList();
    }

    @Override
    public synchronized void markSent(long id) {
      outboxRows.removeIf(r -> r.id() == id);
    }

    @Override
    public int purgeSent(Instant before) {
      return 0;
    }

    @Override
    public long pendingCount(String kind) {
      return pending(kind, Integer.MAX_VALUE).size();
    }

    @Override
    public double oldestAgeSeconds(String kind) {
      return 0;
    }
  };

  public final AdminLog adminLog = new AdminLog() {
    @Override
    public synchronized void record(String actor, String action, String userId, String reason) {
      adminActions.add(actor + " " + action + " " + userId + " " + reason);
      adminActionsInTx.add(inTx);
    }

    @Override
    public List<Entry> of(String userId) {
      return List.of();
    }
  };

  /** Revocations written to the outbox for a user: "device:<id>" or "principal". */
  public synchronized List<String> revocations(String userId) {
    return outboxRows.stream().filter(r -> r.kind().equals(Outbox.REVOCATION) && r.key().equals(userId))
        .map(r -> new String(r.payload(), StandardCharsets.UTF_8)).toList();
  }

  /** AccountState statuses written to the outbox for a user, in order. */
  public synchronized List<AccountState.Status> events(String userId) {
    return outboxRows.stream().filter(r -> r.kind().equals(Outbox.EVENT) && r.key().equals(userId)).map(r -> {
      try {
        return AccountState.parseFrom(r.payload()).getStatus();
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw new IllegalStateException(e);
      }
    }).toList();
  }
}
