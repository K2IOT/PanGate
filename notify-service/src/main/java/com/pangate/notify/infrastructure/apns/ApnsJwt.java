package com.pangate.notify.infrastructure.apns;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.security.interfaces.ECPrivateKey;
import java.util.Date;
import java.util.function.LongSupplier;

/** The APNs provider token (spec §5.5): ES256, {@code kid} = key id, {@code iss} = team id; renewed every 50 minutes. */
public final class ApnsJwt {

  static final long RENEW_MS = 50 * 60_000L;

  private final ECPrivateKey key;
  private final String keyId;
  private final String teamId;
  private final LongSupplier wallMs;
  private String token;          // guarded by this
  private long issuedAtMs;       // guarded by this

  public ApnsJwt(ECPrivateKey key, String keyId, String teamId, LongSupplier wallMs) {
    this.key = key;
    this.keyId = keyId;
    this.teamId = teamId;
    this.wallMs = wallMs;
  }

  public synchronized String token() {
    long now = wallMs.getAsLong();
    if (token == null || now - issuedAtMs >= RENEW_MS) {
      try {
        SignedJWT j = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(keyId).build(),
            new JWTClaimsSet.Builder().issuer(teamId).issueTime(new Date(now / 1000 * 1000)).build());
        j.sign(new ECDSASigner(key));
        token = j.serialize();
        issuedAtMs = now;
      } catch (Exception e) {
        throw new IllegalStateException("APNs token not signed", e);
      }
    }
    return token;
  }

  /** APNs answered ExpiredProviderToken: sign a new one on the next call. */
  public synchronized void forget() {
    token = null;
  }
}
