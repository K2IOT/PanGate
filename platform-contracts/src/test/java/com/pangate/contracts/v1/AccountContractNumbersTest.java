package com.pangate.contracts.v1;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** account.proto field numbers are part of the wire contract (account spec §6). */
class AccountContractNumbersTest {

  @Test
  void fieldNumbersArePinned() {
    assertThat(AccountState.USER_ID_FIELD_NUMBER).isEqualTo(1);
    assertThat(AccountState.STATUS_FIELD_NUMBER).isEqualTo(2);
    assertThat(AccountState.UPDATED_AT_MS_FIELD_NUMBER).isEqualTo(3);
    assertThat(AccountState.Status.STATUS_ACTIVE_VALUE).isEqualTo(1);
    assertThat(AccountState.Status.STATUS_LOCKED_VALUE).isEqualTo(2);
    assertThat(AccountState.Status.STATUS_DELETED_VALUE).isEqualTo(3);
  }
}
