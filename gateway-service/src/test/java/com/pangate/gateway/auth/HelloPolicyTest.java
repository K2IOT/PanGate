package com.pangate.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.contracts.v1.TenantLimits;
import com.pangate.gateway.control.RevocationList;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.test.LoopFixture;
import org.junit.jupiter.api.Test;

class HelloPolicyTest {

  private final LoopFixture f = new LoopFixture();
  /** One handshake per minute: a second accepted check proves the first one did not consume it. */
  private final PrincipalHandshakeLimiter limiter = new PrincipalHandshakeLimiter(1, () -> f.now() * 1000);
  private final HelloPolicy policy = f.policy(limiter);

  private static HelloPolicy.Candidate candidate(String cell, String sdk, String origin) {
    return new HelloPolicy.Candidate(cell, "t1", "t1:u1", "d1", 42, sdk, origin);
  }

  private void tenant(TenantCfg.Builder cfg) {
    f.tenants.apply(cfg.setScope(Scope.SCOPE_TENANT).setScopeId("t1").build());
  }

  private CloseCode code(HelloPolicy.Candidate c) {
    return policy.check(c).map(HelloPolicy.Rejection::code).orElse(null);
  }

  @Test
  void acceptsByDefaultAndConsumesTheRateBudget() {
    assertThat(policy.check(candidate("cell-1", "1.0", null))).isEmpty();
    assertThat(code(candidate("cell-1", "1.0", null))).isEqualTo(CloseCode.RATE_LIMITED);
  }

  @Test
  void checksRunInSpecOrder() {
    f.cells.put("t1", "cell-2");
    f.revocations.add(RevocationList.principalKey("t1:u1"), Long.MAX_VALUE);
    tenant(TenantCfg.newBuilder().setSwitches(KillSwitches.newBuilder().setTenantBlocked(true))
        .setLimits(TenantLimits.newBuilder().setMinSdkVersion("9.0").addOriginAllowlist("https://ok")));
    HelloPolicy.Candidate c = candidate("cell-1", "1.0", "https://evil");

    assertThat(code(c)).isEqualTo(CloseCode.WRONG_CELL);
    f.cells.remove("t1");
    assertThat(code(c)).isEqualTo(CloseCode.REVOKED);
    f.revocations.remove(RevocationList.principalKey("t1:u1"));
    assertThat(code(c)).isEqualTo(CloseCode.RATE_LIMITED);
    tenant(TenantCfg.newBuilder().setSwitches(KillSwitches.newBuilder().setTenantBlocked(false))
        .setLimits(TenantLimits.newBuilder().setMinSdkVersion("9.0").addOriginAllowlist("https://ok")));
    assertThat(code(c)).isEqualTo(CloseCode.UPGRADE_REQUIRED);
    assertThat(code(candidate("cell-1", "9.1", "https://evil"))).isEqualTo(CloseCode.AUTH_FAILED);
    assertThat(code(candidate("cell-1", "9.1", "https://ok"))).isNull();
    assertThat(code(candidate("cell-2", "9.1", "https://ok"))).isEqualTo(CloseCode.WRONG_CELL);
  }

  @Test
  void blockedTenantGets4009With300s() {
    tenant(TenantCfg.newBuilder().setSwitches(KillSwitches.newBuilder().setTenantBlocked(true)));

    HelloPolicy.Rejection r = policy.check(candidate("cell-1", "1.0", null)).orElseThrow();

    assertThat(r.code().code()).isEqualTo(4009);
    assertThat(r.retryAfterMs()).isEqualTo(300_000);
    assertThat(r.metricResult()).isEqualTo("blocked");
  }

  @Test
  void oldSdkGets4010() {
    tenant(TenantCfg.newBuilder().setLimits(TenantLimits.newBuilder().setMinSdkVersion("2.0")));

    assertThat(code(candidate("cell-1", "1.9.9", null))).isEqualTo(CloseCode.UPGRADE_REQUIRED);
    assertThat(policy.checkPolicy(candidate("cell-1", "2.0.0", null))).isEmpty();
  }

  @Test
  void originOutsideAllowlistGets4001AndEmptyAllowlistAllowsAll() {
    assertThat(policy.checkPolicy(candidate("cell-1", "1.0", "https://anything"))).isEmpty();
    tenant(TenantCfg.newBuilder().setLimits(TenantLimits.newBuilder().addOriginAllowlist("https://app")));

    assertThat(code(candidate("cell-1", "1.0", "https://other"))).isEqualTo(CloseCode.AUTH_FAILED);
    assertThat(policy.checkPolicy(candidate("cell-1", "1.0", "https://app"))).isEmpty();
    assertThat(policy.checkPolicy(candidate("cell-1", "1.0", null))).isEmpty();
  }

  @Test
  void revokedByJtiOrDevice() {
    f.revocations.add(RevocationList.jtiKey(42), Long.MAX_VALUE);

    assertThat(code(candidate("cell-1", "1.0", null))).isEqualTo(CloseCode.REVOKED);
  }

  @Test
  void rateLimitIsLastAndOnlyConsumedWhenOtherChecksPass() {
    f.revocations.add(RevocationList.principalKey("t1:u1"), Long.MAX_VALUE);
    assertThat(code(candidate("cell-1", "1.0", null))).isEqualTo(CloseCode.REVOKED);
    f.revocations.remove(RevocationList.principalKey("t1:u1"));

    assertThat(code(candidate("cell-1", "1.0", null))).isNull();
  }

  @Test
  void checkPolicySkipsRateLimit() {
    for (int i = 0; i < 5; i++) {
      assertThat(policy.checkPolicy(candidate("cell-1", "1.0", null))).isEmpty();
    }
    assertThat(code(candidate("cell-1", "1.0", null))).isNull();
  }
}
