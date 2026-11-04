package com.pangate.gateway.tenant;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.contracts.v1.TenantLimits;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.LoopFixture;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class TenantCapRecountTest {

  private final LoopFixture f = new LoopFixture();

  private Session active(String tenant, String principal) {
    EmbeddedChannel ch = new EmbeddedChannel();
    Session s = f.tracker.admit(ch, f.loop, InetAddress.getLoopbackAddress());
    s.activate(new Session.Identity(tenant, principal, "d1", "ios", "1.0", Set.of()), f.tracker.newConnId(s), 0L,
        f.wallNow(), f.wallNow() + 3600, 1, List.of());
    return s;
  }

  @Test
  void enablingACapCountsTheTenantsOpenSessions() {
    TenantConnCounter counter = new TenantConnCounter();
    TenantCap cap = new TenantCap(counter, () -> 1, (t, m, o) -> CompletableFuture.completedFuture(true), f.metrics,
        f.loops, f.tenants);
    Session a = active("t1", "t1:a");
    Session b = active("t1", "t1:b");
    Session other = active("t2", "t2:a");

    TenantCfg cfg = TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
        .setLimits(TenantLimits.newBuilder().setMaxConns(5)).build();
    f.tenants.apply(cfg);
    cap.onTenantCfg(cfg);
    f.channel.runPendingTasks();

    assertThat(counter.count("t1")).isEqualTo(2);
    assertThat(a.tenantReserved()).isTrue();
    assertThat(b.tenantReserved()).isTrue();
    assertThat(other.tenantReserved()).isFalse();

    cap.onTenantCfg(cfg);  // idempotent
    f.channel.runPendingTasks();
    assertThat(counter.count("t1")).isEqualTo(2);
  }
}
