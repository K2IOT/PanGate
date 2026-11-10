package com.pangate.contracts.testkit;

import com.pangate.contracts.v1.PushRecord;
import com.pangate.contracts.v1.PushRequest;

/**
 * PushRequest.Kind (CONTROL = 1, MESSAGE = 2, BROADCAST = 3) and PushRecord.Kind (MESSAGE = 1, BROADCAST = 2,
 * CONTROL = 3) number the same kinds differently: map by meaning, never with {@code forNumber(getKindValue())}.
 */
public final class PushKinds {

  private PushKinds() {}

  public static PushRecord.Kind toRecord(PushRequest.Kind k) {
    return switch (k) {
      case KIND_MESSAGE -> PushRecord.Kind.KIND_MESSAGE;
      case KIND_BROADCAST -> PushRecord.Kind.KIND_BROADCAST;
      case KIND_CONTROL -> PushRecord.Kind.KIND_CONTROL;
      default -> throw new IllegalArgumentException("no record kind for " + k);
    };
  }
}
