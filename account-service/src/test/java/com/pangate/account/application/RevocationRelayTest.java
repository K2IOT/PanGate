package com.pangate.account.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.account.application.port.AuthApi;
import com.pangate.account.application.port.AuthApi.AuthRejectedException;
import com.pangate.account.application.port.AuthApi.AuthUnavailableException;
import com.pangate.account.application.port.Metrics;
import com.pangate.account.support.Fakes;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RevocationRelayTest {

  final Fakes f = new Fakes();
  final List<String> sent = new ArrayList<>();
  final List<String> counted = new ArrayList<>();

  RevocationRelay relay(AuthApi api) {
    return new RevocationRelay(f.outbox, api, 500, (name, tags) -> counted.add(name));
  }

  static AuthApi failingAt(String payload, RuntimeException e, List<String> sent, AtomicInteger calls) {
    return new AuthApi() {
      @Override
      public ConnectToken mint(String userId, String deviceId) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void revoke(byte[] json) {
        calls.incrementAndGet();
        String s = new String(json, StandardCharsets.UTF_8);
        if (s.equals(payload)) {
          throw e;
        }
        sent.add(s);
      }
    };
  }

  @Test
  void transientErrorStopsTheRoundInOrder() {
    f.outbox.revokeDevice("u", "d1");
    f.outbox.revokeDevice("u", "d2");
    f.outbox.revokeDevice("u", "d3");
    AtomicInteger calls = new AtomicInteger();
    assertThat(relay(failingAt("device:d2", new AuthUnavailableException("down"), sent, calls)).runOnce()).isEqualTo(1);
    assertThat(sent).containsExactly("device:d1");
    assertThat(calls.get()).isEqualTo(2);
    assertThat(relay(failingAt("none", null, sent, calls)).runOnce()).isEqualTo(2);
    assertThat(sent).containsExactly("device:d1", "device:d2", "device:d3");
  }

  @Test
  void permanentErrorIsSkipped() {
    f.outbox.revokeDevice("u", "d1");
    f.outbox.revokeDevice("u", "d2");
    f.outbox.revokeDevice("u", "d3");
    assertThat(relay(failingAt("device:d2", new AuthRejectedException(403), sent, new AtomicInteger())).runOnce())
        .isEqualTo(3);
    assertThat(sent).containsExactly("device:d1", "device:d3");
    assertThat(f.outbox.pendingCount("revocation")).isZero();
    assertThat(counted).containsExactly("account.revocations.rejected");
  }

  @Test
  void metricsNoneIsANoOp() {
    Metrics.NONE.count("x");
  }
}
