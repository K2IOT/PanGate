package io.lettuce.core.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.test.BlockHoundExtension;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * BlockHound treats Netty threads, Lettuce's own event loop included, as non-blocking. On close Lettuce takes
 * SharedLock exclusively on its event loop; if the test thread holds it (closing the same endpoint) that thread
 * parks for microseconds. This flaked TenantCapBorrowerIT teardown once in M3b.
 */
class SharedLockBlockHoundTest {

  @Test
  void exclusiveLockContentionOnALettuceThreadIsAllowed() throws Exception {
    BlockHoundExtension.install();
    SharedLock lock = new SharedLock();
    CountDownLatch held = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Thread holder = new Thread(() -> lock.doExclusive(() -> {
      held.countDown();
      try {
        release.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }));
    holder.start();
    assertThat(held.await(5, TimeUnit.SECONDS)).isTrue();

    AtomicReference<Throwable> error = new AtomicReference<>();
    Thread lettuceLoop = new FastThreadLocalThread(() -> {
      try {
        lock.doExclusive(() -> { });
      } catch (Throwable t) {
        error.set(t);
      }
    }, "lettuce-nioEventLoop-4-1");
    lettuceLoop.start();
    Thread.sleep(200);  // lettuceLoop is now parked on the lock
    release.countDown();
    lettuceLoop.join(5000);
    holder.join(5000);

    assertThat(error.get()).as("blocking error on a Lettuce event loop thread").isNull();
  }
}
