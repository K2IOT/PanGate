package com.pangate.account.infrastructure.crypto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class HashGateTest {

  @Test
  void busyWhenAllPermitsTaken() throws Exception {
    HashGate gate = new HashGate(1);
    CountDownLatch inside = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Thread t = Thread.ofVirtual().start(() -> gate.run(() -> {
      inside.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return 1;
    }));
    assertThat(inside.await(5, TimeUnit.SECONDS)).isTrue();
    assertThatThrownBy(() -> gate.run(() -> 2)).isInstanceOf(BusyException.class);
    release.countDown();
    t.join();
    assertThat(gate.run(() -> 3)).isEqualTo(3);
  }

  @Test
  void defaultPermitsAreTwicePerCore() {
    assertThat(HashGate.permits(0)).isEqualTo(2 * Runtime.getRuntime().availableProcessors());
    assertThat(HashGate.permits(3)).isEqualTo(3);
  }
}
