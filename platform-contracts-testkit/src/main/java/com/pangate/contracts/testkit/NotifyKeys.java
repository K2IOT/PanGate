package com.pangate.contracts.testkit;

import com.pangate.contracts.v1.NotifyRequest;

/** Kafka key of a NotifyRequest: the recipient principal, so one person's requests stay in order (spec §2.1). */
public final class NotifyKeys {

  private NotifyKeys() {}

  public static String of(NotifyRequest r) {
    return r.getTargetCase() == NotifyRequest.TargetCase.DEVICE ? r.getDevice().getPrincipalId() : r.getPrincipalId();
  }
}
