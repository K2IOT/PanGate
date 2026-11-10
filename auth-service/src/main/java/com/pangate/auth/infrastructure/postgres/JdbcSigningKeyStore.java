package com.pangate.auth.infrastructure.postgres;

import com.nimbusds.jose.jwk.ECKey;
import com.pangate.auth.application.port.KeyCipher;
import com.pangate.auth.application.port.SigningKeyStore;
import com.pangate.auth.domain.KeyState;
import com.pangate.auth.domain.SigningKey;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;

/** {@code auth.signing_keys}; private keys are sealed with {@link KeyCipher} and never logged. */
public class JdbcSigningKeyStore implements SigningKeyStore {

  private static final Logger log = LoggerFactory.getLogger(JdbcSigningKeyStore.class);

  private final JdbcClient jdbc;
  private final KeyCipher cipher;
  private final Counter undecryptable;

  public JdbcSigningKeyStore(JdbcClient jdbc, KeyCipher cipher, MeterRegistry metrics) {
    this.jdbc = jdbc;
    this.cipher = cipher;
    this.undecryptable = metrics.counter("auth.keys.undecryptable");
  }

  private record Row(String kid, byte[] sealed, String kekId, String state, Timestamp activateAt, Timestamp retireAt,
      Timestamp removeAt) {}

  @Override
  public List<SigningKey> all() {
    List<Row> rows = jdbc.sql("SELECT kid, private_key_enc, kek_id, state, activate_at, retire_at, remove_at"
        + " FROM auth.signing_keys ORDER BY activate_at").query((rs, i) -> new Row(rs.getString(1), rs.getBytes(2),
        rs.getString(3), rs.getString(4), rs.getTimestamp(5), rs.getTimestamp(6), rs.getTimestamp(7))).list();
    List<SigningKey> out = new ArrayList<>(rows.size());
    for (Row r : rows) {
      try {
        ECKey jwk = ECKey.parse(new String(cipher.decrypt(r.kekId, r.kid, r.sealed), StandardCharsets.UTF_8));
        out.add(new SigningKey(r.kid, jwk, KeyState.fromDb(r.state), instant(r.activateAt), instant(r.retireAt),
            instant(r.removeAt)));
      } catch (Exception e) {
        undecryptable.increment();
        log.error("signing key {} (KEK {}) cannot be decrypted and is not used: {}", r.kid, r.kekId,
            e.getClass().getSimpleName());
      }
    }
    return out;
  }

  @Override
  public void insert(SigningKey k) {
    byte[] sealed = cipher.encrypt(k.kid(), k.privateJwk().toJSONString().getBytes(StandardCharsets.UTF_8));
    jdbc.sql("INSERT INTO auth.signing_keys (kid, private_key_enc, kek_id, public_jwk, state, activate_at, retire_at,"
        + " remove_at) VALUES (:kid, :enc, :kek, CAST(:pub AS jsonb), :state, :act, :ret, :rem)")
        .param("kid", k.kid()).param("enc", sealed).param("kek", cipher.currentKekId())
        .param("pub", k.privateJwk().toPublicJWK().toJSONString()).param("state", k.state().db())
        .param("act", ts(k.activateAt())).param("ret", ts(k.retireAt())).param("rem", ts(k.removeAt()))
        .update();
  }

  @Override
  public void update(SigningKey k) {
    jdbc.sql("UPDATE auth.signing_keys SET state = :state, retire_at = :ret, remove_at = :rem WHERE kid = :kid")
        .param("state", k.state().db()).param("ret", ts(k.retireAt())).param("rem", ts(k.removeAt()))
        .param("kid", k.kid()).update();
  }

  @Override
  public void delete(String kid) {
    jdbc.sql("DELETE FROM auth.signing_keys WHERE kid = :kid").param("kid", kid).update();
  }

  @Override
  public int retireActive(Instant retireAt, Instant removeAt) {
    return jdbc.sql("UPDATE auth.signing_keys SET state = 'retired', retire_at = :ret, remove_at = :rem"
        + " WHERE state = 'active'").param("ret", ts(retireAt)).param("rem", ts(removeAt)).update();
  }

  /** KEK ids referenced by stored keys; each must be loadable at startup (spec §3). */
  public List<String> referencedKekIds() {
    return jdbc.sql("SELECT DISTINCT kek_id FROM auth.signing_keys").query(String.class).list();
  }

  /** Truncated to microseconds: PostgreSQL rounds, which could move activate_at past the instant it was meant for. */
  private static Timestamp ts(Instant i) {
    return i == null ? null : Timestamp.from(i.truncatedTo(ChronoUnit.MICROS));
  }

  private static Instant instant(Timestamp t) {
    return t == null ? null : t.toInstant();
  }
}
