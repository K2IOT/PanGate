package com.pangate.account.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.account.application.port.Metrics;
import com.pangate.account.domain.AccountStatus;
import com.pangate.account.domain.DeviceInfo;
import com.pangate.account.domain.DomainException;
import com.pangate.account.infrastructure.crypto.HashGate;
import com.pangate.account.infrastructure.redis.InMemoryAttemptCounter;
import com.pangate.account.support.Fakes;
import com.pangate.account.support.TestLimits;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ProfilesTest {

  final Fakes f = new Fakes();
  final InMemoryAttemptCounter counter = new InMemoryAttemptCounter(f.clock);
  final Register register = new Register(f.accountStore, f.sessionStore, f.outbox, f.hasher, new HashGate(2), counter,
      TestLimits.SPEC, Set.of(), f.tx, Metrics.NONE);
  final Profiles profiles = new Profiles(f.accountStore, Set.of("admin"), counter, TestLimits.SPEC);

  NewSession user(String name) {
    return register.register(name, "secret-pw".toCharArray(), name + "@x.vn", null, null, DeviceInfo.of("p", "ios"),
        "1.1.1.1");
  }

  Caller caller(NewSession s) {
    return new Caller(s.userId(), s.deviceId(), "1.1.1.1");
  }

  @Test
  void batchSkipsUnknownIdsAndMarksDeleted() {
    NewSession a = user("alice1");
    NewSession b = user("bobby1");
    f.accountStore.erase(b.userId());
    List<Profiles.PublicProfile> out = profiles.batch(caller(a), List.of(a.userId(), "01NOPE", b.userId()));
    assertThat(out).hasSize(2);
    assertThat(out.get(0).username()).isEqualTo("alice1");
    assertThat(out.get(0).deleted()).isFalse();
    assertThat(out.get(1).userId()).isEqualTo(b.userId());
    assertThat(out.get(1).deleted()).isTrue();
    assertThat(out.get(1).username()).isNull();
  }

  @Test
  void batchOver100Is400() {
    NewSession a = user("alice2");
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < 101; i++) {
      ids.add("id" + i);
    }
    assertThatThrownBy(() -> profiles.batch(caller(a), ids)).isInstanceOfSatisfying(DomainException.class,
        e -> assertThat(e.code()).isEqualTo("invalid_request"));
  }

  @Test
  void lockedUserStillHasAProfile() {
    NewSession a = user("alice3");
    f.accountStore.setStatus(a.userId(), AccountStatus.LOCKED);
    assertThat(profiles.byId(caller(a), a.userId()).username()).isEqualTo("alice3");
    assertThatThrownBy(() -> profiles.byId(caller(a), "01NOPE")).isInstanceOfSatisfying(DomainException.class,
        e -> assertThat(e.code()).isEqualTo("not_found"));
  }

  @Test
  void updateKeepsWhatIsNotSentAndRenameIsChecked() {
    NewSession a = user("alice4");
    profiles.update(caller(a), "Alice", null);
    profiles.update(caller(a), null, "hi");
    assertThat(profiles.me(caller(a))).satisfies(m -> {
      assertThat(m.displayName()).isEqualTo("Alice");
      assertThat(m.bio()).isEqualTo("hi");
      assertThat(m.email()).isEqualTo("alice4@x.vn");
    });
    assertThatThrownBy(() -> profiles.changeUsername(caller(a), "admin")).isInstanceOf(DomainException.class);
    user("taken55");
    assertThatThrownBy(() -> profiles.changeUsername(caller(a), "TAKEN55")).isInstanceOfSatisfying(
        DomainException.class, e -> assertThat(e.code()).isEqualTo("username_taken"));
  }
}
