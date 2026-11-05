package io.lettuce.core.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.test.BlockHoundExtension;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockingOperationError;

/**
 * Netty's BlockHound integration treats every Netty thread as non-blocking, Lettuce's own event loop included. On
 * close Lettuce takes SharedLock exclusively there; if another thread holds it (closing the same endpoint), the loop
 * parks for microseconds. This flaked TenantCapBorrowerIT teardown once in M3b. Only gw-io-* threads are ours to
 * police: Lettuce's threads are exempt, while the same contention on a gateway loop must still be caught.
 */
class SharedLockBlockHoundTest {

  @Test
  void exclusiveLockContentionOnALettuceThreadIsAllowed() throws Exception {
    assertThat(contendOn("lettuce-nioEventLoop-4-1")).as("blocking error on a Lettuce event loop thread").isNull();
  }

  @Test
  void exclusiveLockContentionOnAGatewayLoopIsCaught() throws Exception {
    assertThat(contendOn("gw-io-7")).isInstanceOf(BlockingOperationError.class);
  }

  /** Runs SharedLock.doExclusive on a Netty thread with the given name while another thread holds the lock. */
  private static Throwable contendOn(String threadName) throws Exception {
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
    CountDownLatch done = new CountDownLatch(1);
    Thread contender = new FastThreadLocalThread(() -> {
      try {
        lock.doExclusive(() -> { });
      } catch (Throwable t) {
        error.set(t);
      } finally {
        done.countDown();
      }
    }, threadName);
    contender.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    // Contended: either parked on the lock, or already stopped by BlockHound.
    while (contender.getState() != Thread.State.WAITING && done.getCount() > 0 && System.nanoTime() < deadline) {
      Thread.onSpinWait();
    }
    release.countDown();
    contender.join(5000);
    holder.join(5000);
    return error.get();
  }
}
