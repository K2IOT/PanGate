package com.pangate.pushapi.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Final review: after a failover every regressing request asked for its own full watcher pass, one after another. */
class SingleFlightTest {

  final AtomicInteger passes = new AtomicInteger();
  final CountDownLatch inFirst = new CountDownLatch(1);
  final CountDownLatch releaseFirst = new CountDownLatch(1);
  final SingleFlight flight = new SingleFlight();

  void work() {
    if (passes.incrementAndGet() == 1) {
      inFirst.countDown();
      try {
        releaseFirst.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Test
  void callersThatArriveDuringAPassShareTheNextOne() throws Exception {
    Thread first = Thread.ofVirtual().start(() -> flight.run(this::work));
    inFirst.await();
    List<Thread> waiting = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      waiting.add(Thread.ofVirtual().start(() -> flight.run(this::work)));
    }
    await().atMost(Duration.ofSeconds(5)).until(() -> waiting.stream().allMatch(t -> t.getState() == Thread.State.WAITING
        || t.getState() == Thread.State.BLOCKED));
    releaseFirst.countDown();
    first.join();
    for (Thread t : waiting) {
      t.join();
    }
    assertThat(passes).hasValue(2);   // the running pass may have missed them; one more covers all twenty
  }

  @Test
  void aCallAfterAPassFinishedRunsAnotherOne() {
    releaseFirst.countDown();
    flight.run(this::work);
    flight.run(this::work);
    assertThat(passes).hasValue(2);
  }
}
