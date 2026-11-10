package com.pangate.auth.diagnostics;

import com.pangate.auth.infrastructure.crypto.KekException;
import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

/** Fail-fast message for a missing or malformed KEK (spec §6): which secret, where it was looked for. */
public class KekFailureAnalyzer extends AbstractFailureAnalyzer<KekException> {

  @Override
  protected FailureAnalysis analyze(Throwable root, KekException e) {
    return new FailureAnalysis(
        "The key-encryption key " + e.secretName() + " " + e.problem() + " (looked for at " + e.location() + ").",
        "Provide exactly 32 random bytes as secret " + e.secretName() + ", e.g. `head -c 32 /dev/urandom > "
            + e.location() + "`, or point auth.keys.kek-id / pangate.secrets.* at the right secret. Profiles local "
            + "and dev create it automatically.", e);
  }
}
