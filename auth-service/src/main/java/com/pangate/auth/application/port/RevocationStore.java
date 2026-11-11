package com.pangate.auth.application.port;

import com.pangate.auth.domain.Revocation;
import java.time.Instant;
import java.util.List;

/** {@code auth.revocations}. */
public interface RevocationStore {

  /** True if an unexpired revocation of kind {@code jti} exists for {@code jti} (indexed lookup). */
  boolean isJtiRevoked(String jti);

  /** Inserts, or extends the row with the same control key (same id, new expiry). Runs inside a transaction. */
  Revocation upsert(Revocation r);

  List<Revocation> expired(Instant now, int limit);

  void delete(long id);
}
