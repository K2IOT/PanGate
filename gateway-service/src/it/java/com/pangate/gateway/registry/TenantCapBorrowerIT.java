package com.pangate.gateway.registry;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.tenant.TenantCapBorrower;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TenantCapBorrowerIT {

  private RedisFixture fx;
  private TenantCapBorrower n1;
  private TenantCapBorrower n2;

  @BeforeEach
  void setUp() {
    fx = new RedisFixture();
    n1 = new TenantCapBorrower(fx.redis, "n1", System::currentTimeMillis);
    n2 = new TenantCapBorrower(fx.redis, "n2", System::currentTimeMillis);
  }

  @AfterEach
  void tearDown() {
    fx.close();
  }

  private static boolean get(java.util.concurrent.CompletionStage<Boolean> s) throws Exception {
    return s.toCompletableFuture().get(2, TimeUnit.SECONDS);
  }

  @Test
  void borrowSumsOtherNodesAndRejectsAboveMax() throws Exception {
    n2.report(Map.of("t1", 5), t -> 6).toCompletableFuture().get(2, TimeUnit.SECONDS);
    assertThat(get(n1.borrow("t1", 6, 1))).isTrue();
    assertThat(get(n1.borrow("t1", 6, 2))).isFalse();
    assertThat(fx.sync.hget("tenantconns:{t1}", "n1")).startsWith("1|");
  }

  @Test
  void expiredNodeEntriesAreIgnored() throws Exception {
    fx.sync.hset("tenantconns:{t1}", "n2", "5|" + (System.currentTimeMillis() - 1));  // n2 was SIGKILLed
    assertThat(get(n1.borrow("t1", 6, 6))).isTrue();
  }

  @Test
  void reportOfZeroRemovesTheField() throws Exception {
    n1.report(Map.of("t1", 3), t -> 10).toCompletableFuture().get(2, TimeUnit.SECONDS);
    assertThat(fx.sync.hexists("tenantconns:{t1}", "n1")).isTrue();
    n1.report(Map.of("t1", 0), t -> 10).toCompletableFuture().get(2, TimeUnit.SECONDS);
    assertThat(fx.sync.hexists("tenantconns:{t1}", "n1")).isFalse();
  }

  @Test
  void tenantsWithoutCapAreNotReported() throws Exception {
    n1.report(Map.of("t9", 3), t -> 0).toCompletableFuture().get(2, TimeUnit.SECONDS);
    assertThat(fx.sync.exists("tenantconns:{t9}")).isZero();
  }
}
