package com.pangate.gateway.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.Scope;
import com.pangate.gateway.auth.ResumeTicketService;
import org.junit.jupiter.api.Test;

class ControlKeysTest {

  @Test
  void parsesAllKeyShapes() {
    assertThat(ControlKeys.parse("revoke:p:t1:u1")).isEqualTo(new ControlKeys.RevokeKey("p:t1:u1"));
    assertThat(ControlKeys.parse("revoke:d:t1:u1/d1")).isEqualTo(new ControlKeys.RevokeKey("d:t1:u1/d1"));
    assertThat(ControlKeys.parse("revoke:j:abc"))
        .isEqualTo(new ControlKeys.RevokeKey("j:" + ResumeTicketService.jtiHash("abc")));
    assertThat(ControlKeys.parse("cellmap:t1")).isEqualTo(new ControlKeys.CellMapKey("t1"));
    assertThat(ControlKeys.parse("tenantcfg:fleet")).isEqualTo(new ControlKeys.TenantCfgKey(Scope.SCOPE_FLEET, ""));
    assertThat(ControlKeys.parse("tenantcfg:cell:cell-1"))
        .isEqualTo(new ControlKeys.TenantCfgKey(Scope.SCOPE_CELL, "cell-1"));
    assertThat(ControlKeys.parse("tenantcfg:tenant:t1"))
        .isEqualTo(new ControlKeys.TenantCfgKey(Scope.SCOPE_TENANT, "t1"));
  }

  @Test
  void malformedKeysAreUnknown() {
    for (String k : new String[] {"garbage", "revoke:x:1", "revoke:p:", "cellmap:", "tenantcfg:cell:", "", null}) {
      assertThat(ControlKeys.parse(k)).as(String.valueOf(k)).isInstanceOf(ControlKeys.Unknown.class);
    }
  }
}
