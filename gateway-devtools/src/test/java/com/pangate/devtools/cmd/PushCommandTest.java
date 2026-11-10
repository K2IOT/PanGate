package com.pangate.devtools.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PushCommandTest {

  @Test
  void withPrincipalsTheIndexIsAppendedRoundRobin() {
    assertThat(PushCommand.principalFor("t1:load-", 0, 1000)).isEqualTo("t1:load-0");
    assertThat(PushCommand.principalFor("t1:load-", 1234, 1000)).isEqualTo("t1:load-234");
  }

  @Test
  void withoutPrincipalsThePrincipalIsUsedAsIs() {
    assertThat(PushCommand.principalFor("t1:u1", 7, 0)).isEqualTo("t1:u1");
  }
}
