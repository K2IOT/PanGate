package com.pangate.gateway.registry;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.cluster.SlotHash;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RedisSeqReaderIT {

  private RedisFixture fx;
  private final ScheduledExecutorService tick = Executors.newSingleThreadScheduledExecutor();
  private RedisSeqReader reader;

  @BeforeEach
  void setUp() {
    // Command timeout far above 200 ms (30 s): an answer within 5 s below proves the reader's own budget cut it short,
    // with room for a CI runner that stalls the JVM for a second or two (one such stall failed a 2 s budget).
    fx = new RedisFixture(Duration.ofSeconds(30));
    reader = new RedisSeqReader(fx.redis, tick, Duration.ofMillis(200));
  }

  @AfterEach
  void tearDown() {
    tick.shutdownNow();
    fx.close();
  }

  private OptionalLong read(String p) throws Exception {
    return reader.read(p).toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  @Test
  void readsSeqAndMissingKeyIsZero() throws Exception {
    fx.sync.set("seq:{t1:u1}", "41");

    assertThat(read("t1:u1")).hasValue(41);
    assertThat(read("t1:nobody")).hasValue(0);
  }

  @Test
  void malformedSeqIsUnknown() throws Exception {
    fx.sync.set("seq:{t1:u2}", "not-a-number");

    assertThat(read("t1:u2")).isEmpty();
  }

  @Test
  void seqEpochUsesClusterSlotOfPrincipal() {
    fx.sync.hset("seqepochs", Integer.toString(SlotHash.getSlot("t1:u1")), "5");
    fx.sync.hset("seqepochs", "garbage", "9");

    assertThat(reader.seqEpoch("t1:u1")).isZero();
    reader.refreshEpochs();

    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> reader.seqEpoch("t1:u1") == 5);
    assertThat(reader.seqEpoch("t1:other-principal-x")).isZero();
  }

  @Test
  void timesOutAfter200msAsEmpty() throws Exception {
    RedisFixture.pause();
    try {
      long t0 = System.nanoTime();
      OptionalLong r = read("t1:u1");

      assertThat(r).isEmpty();
      assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(5));
    } finally {
      RedisFixture.unpause();
    }
  }
}
