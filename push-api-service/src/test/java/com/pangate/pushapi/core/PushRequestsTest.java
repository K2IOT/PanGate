package com.pangate.pushapi.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.testkit.PushRequestCases;
import com.pangate.contracts.v1.PushResponse;
import org.junit.jupiter.api.Test;

class PushRequestsTest {

  @Test
  void everyContractCaseGetsItsCode() {
    for (var c : PushRequestCases.all()) {
      var v = PushRequests.violation(c.request());
      assertThat(v.isEmpty()).as(c.name() + " → " + v).isEqualTo(c.expected() == PushResponse.Code.CODE_OK);
      v.ifPresent(x -> assertThat(x.rule()).as(c.name()).isEqualTo(c.rule()));
    }
  }
}
