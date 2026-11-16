package com.pangate.account.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.account.application.port.AuthApi;
import com.pangate.account.application.port.Metrics;
import com.pangate.account.domain.AccountStatus;
import com.pangate.account.domain.DeviceInfo;
import com.pangate.account.domain.DomainException;
import com.pangate.account.infrastructure.crypto.HashGate;
import com.pangate.account.infrastructure.redis.InMemoryAttemptCounter;
import com.pangate.account.support.Fakes;
import com.pangate.account.support.TestLimits;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ExchangeTokenTest {

  final Fakes f = new Fakes();
  final InMemoryAttemptCounter counter = new InMemoryAttemptCounter(f.clock);
  final List<String> minted = new ArrayList<>();
  final AuthApi auth = new AuthApi() {
    @Override
    public ConnectToken mint(String userId, String deviceId) {
      minted.add(userId + "/" + deviceId);
      return new ConnectToken("jwt-" + deviceId, Instant.now().plusSeconds(600));
    }

    @Override
    public void revoke(byte[] json) {}
  };
  final ExchangeToken exchange = new ExchangeToken(f.sessionStore, f.accountStore, auth, counter, TestLimits.SPEC,
      f.clock, Duration.ofDays(180), Duration.ofMinutes(5), Metrics.NONE);
  final NewSession s;

  ExchangeTokenTest() {
    f.now = Instant.now();
    s = new Register(f.accountStore, f.sessionStore, f.outbox, f.hasher, new HashGate(2), counter, TestLimits.SPEC,
        Set.of(), f.tx, Metrics.NONE).register("lannguyen", "secret-pw".toCharArray(), "l@x.vn", null, null,
        DeviceInfo.of("p", "ios"), "1.1.1.1");
  }

  String code() {
    try {
      exchange.exchange(s.refreshToken());
      return "ok";
    } catch (DomainException e) {
      return e.code();
    }
  }

  @Test
  void exchangeMintsForTheSessionDevice() {
    assertThat(exchange.exchange(s.refreshToken()).token()).isEqualTo("jwt-" + s.deviceId());
    assertThat(minted).containsExactly(s.userId() + "/" + s.deviceId());
  }

  @Test
  void revokedOrIdleSessionIs401() {
    assertThat(exchangeCode("rt_garbage")).isEqualTo("invalid_session");
    f.now = f.now.plus(Duration.ofDays(181));
    assertThat(code()).isEqualTo("invalid_session");
    f.now = f.now.minus(Duration.ofDays(181));
    f.sessionStore.revoke(s.userId(), s.deviceId());
    assertThat(code()).isEqualTo("invalid_session");
    assertThat(minted).isEmpty();
  }

  String exchangeCode(String token) {
    try {
      exchange.exchange(token);
      return "ok";
    } catch (DomainException e) {
      return e.code();
    }
  }

  @Test
  void lockedIs403AndDeletedIs401() {
    f.accountStore.setStatus(s.userId(), AccountStatus.LOCKED);
    assertThat(code()).isEqualTo("account_locked");
    f.accountStore.setStatus(s.userId(), AccountStatus.ACTIVE);
    f.accountStore.erase(s.userId());
    assertThat(code()).isEqualTo("invalid_session");
  }

  @Test
  void lastUsedIsWrittenAtMostEveryFiveMinutes() {
    f.now = f.now.plus(Duration.ofMinutes(6));
    exchange.exchange(s.refreshToken());
    f.now = f.now.plus(Duration.ofMinutes(1));
    exchange.exchange(s.refreshToken());
    assertThat(f.touches.get()).isEqualTo(1);
  }

  @Test
  void sixtyOneExchangesInAnHourAreLimited() {
    for (int i = 0; i < 60; i++) {
      assertThat(code()).isEqualTo("ok");
    }
    assertThat(code()).isEqualTo("rate_limited");
  }
}
