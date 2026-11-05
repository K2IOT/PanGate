package com.pangate.gateway.test;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import org.junit.jupiter.api.Test;

class LeakWatchTest {

  @Test
  void detectsDroppedBuffer() {
    ResourceLeakDetector.Level before = ResourceLeakDetector.getLevel();
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    try {
      LeakWatch.install();
      LeakWatch.reset();
      dropOne();
      LeakWatch.collect();
      assertThat(LeakWatch.leaks()).isPositive();
      assertThat(LeakWatch.leaks("LeakWatchTest.dropOne")).isPositive();
      assertThat(LeakWatch.leaks("NoSuchTestClass")).isZero();
    } finally {
      LeakWatch.reset();
      ResourceLeakDetector.setLevel(before);
    }
  }

  private static void dropOne() {
    PooledByteBufAllocator.DEFAULT.buffer(128).writeInt(42);  // never released
  }
}
