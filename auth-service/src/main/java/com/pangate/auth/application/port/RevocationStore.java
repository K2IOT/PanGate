package com.pangate.auth.application.port;

import java.util.Set;

/** Revocations as seen by request authentication (extended in Task 9). */
public interface RevocationStore {

  /** jtis of revocations of kind {@code jti} that have not expired. */
  Set<String> activeJtis();
}
