package com.pangate.gateway.registry;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.testkit.RegistryValues;
import org.junit.jupiter.api.Test;

/** Registry value v2 (push contracts spec §4) against the testkit's reference cases. */
class RegistryValueV2Test {

  @Test
  void writerMatchesTheContract() {
    assertThat(RegistryWriter.value("gw-1", 7, 1000, "ios", "dev|with|pipes"))
        .isEqualTo(RegistryValues.v2("gw-1", 7, 1000, "ios", "dev|with|pipes"));
    assertThat(RegistryValues.parse(RegistryWriter.value("gw-1", 7, 1000, "ios", "dev|with|pipes")).orElseThrow()
        .device()).hasValue("dev|with|pipes");
    assertThat(RegistryWriter.value("gw-1", 7, 1000, null, "d1")).isEqualTo("gw-1|7|1000||d1");
  }

  @Test
  void gatewayReadersAcceptV1AndV2() {
    for (var c : RegistryValues.cases()) {
      c.expected().ifPresent(p ->
          assertThat(RegistryWriter.connectedAtMs(c.value())).as(c.name()).isEqualTo(p.connectedAtMs()));
    }
  }
}
