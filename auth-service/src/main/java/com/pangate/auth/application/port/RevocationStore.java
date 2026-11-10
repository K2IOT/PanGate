package com.pangate.auth.application.port;

import com.pangate.auth.domain.Revocation;
import java.time.Instant;
import java.util.List;
import java.util.Set;

/** {@code auth.revocations}. */
public interface RevocationStore {

  /** jtis of revocations of kind {@code jti} that have not expired. */
  Set<String> activeJtis();

  /** Inserts, or extends the row with the same control key (same id, new expiry). Runs inside a transaction. */
  Revocation upsert(Revocation r);

  List<Revocation> expired(Instant now, int limit);

  void delete(long id);
}
