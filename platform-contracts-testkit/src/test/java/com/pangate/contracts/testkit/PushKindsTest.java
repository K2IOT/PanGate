package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.v1.PushRecord;
import com.pangate.contracts.v1.PushRequest;
import org.junit.jupiter.api.Test;

/** PushRequest.Kind and PushRecord.Kind number the same kinds differently; map by name, never by number. */
class PushKindsTest {

  @Test
  void mapsEveryKindByMeaning() {
    assertThat(PushKinds.toRecord(PushRequest.Kind.KIND_MESSAGE)).isEqualTo(PushRecord.Kind.KIND_MESSAGE);
    assertThat(PushKinds.toRecord(PushRequest.Kind.KIND_BROADCAST)).isEqualTo(PushRecord.Kind.KIND_BROADCAST);
    assertThat(PushKinds.toRecord(PushRequest.Kind.KIND_CONTROL)).isEqualTo(PushRecord.Kind.KIND_CONTROL);
    assertThat(PushRequest.Kind.KIND_CONTROL.getNumber()).isNotEqualTo(PushRecord.Kind.KIND_CONTROL.getNumber());
    assertThatThrownBy(() -> PushKinds.toRecord(PushRequest.Kind.KIND_UNSPECIFIED))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
