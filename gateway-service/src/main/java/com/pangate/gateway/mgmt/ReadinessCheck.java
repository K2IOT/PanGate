package com.pangate.gateway.mgmt;

import java.util.Optional;

/** One condition of node readiness (spec §7.9); M2/M3 add more by declaring beans. */
@FunctionalInterface
public interface ReadinessCheck {

  /** Empty when satisfied, otherwise why the node is not ready. */
  Optional<String> notReadyReason();
}
