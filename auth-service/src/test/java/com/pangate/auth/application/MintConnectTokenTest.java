package com.pangate.auth.application;

import static com.pangate.auth.domain.ClaimLimitsTest.assertCode;
import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jwt.SignedJWT;
import com.pangate.auth.domain.Caller;
import com.pangate.auth.domain.Scope;
import com.pangate.auth.infrastructure.crypto.Es256TokenSigner;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MintConnectTokenTest {

  private final Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
  private final InMemoryStores.Tenants tenants = new InMemoryStores.Tenants();
  private final InMemorySigningKeyStore keys = new InMemorySigningKeyStore();
  private final KeyRing ring = new KeyRing(keys);
  private final Caller t1 = new Caller("t1-backend", "t1", Set.of(Scope.TOKENS_MINT), "j");
  private MintConnectToken mint;

  @BeforeEach
  void setUp() {
    new RotateSigningKeys(keys, () -> now, RotateSigningKeysTest.KEYS, Es256TokenSigner::newKey).runOnce();
    ring.reload();
    new ManageTenants(tenants).create("t1");
    new ManageTenants(tenants).create("pangate");
    mint = new MintConnectToken(tenants, new Es256TokenSigner("iss"), ring, () -> now, "iss", Duration.ofMinutes(10));
  }

  @Test
  void tenantComesFromTheCallerNotTheBody() {
    assertCode(() -> mint.mint(t1, "t2:x", "d", List.of(), null), "principal_outside_tenant");
  }

  @Test
  void disabledTenantIsForbidden() {
    tenants.setActive("t1", false);
    assertThat(assertCode(() -> mint.mint(t1, "t1:u", "d", List.of(), null), "tenant_disabled").httpStatus())
        .isEqualTo(403);
  }

  @Test
  void missingScopeIsForbidden() {
    var noMint = new Caller("t1-x", "t1", Set.of(Scope.PUSH_SEND), "j");
    assertThat(assertCode(() -> mint.mint(noMint, "t1:u", "d", List.of(), null), "missing_scope").httpStatus())
        .isEqualTo(403);
  }

  @Test
  void operatorTenantCannotMint() {
    var op = new Caller("pangate-admin", "pangate", Set.of(Scope.TOKENS_MINT), "j");
    assertCode(() -> mint.mint(op, "pangate:u", "d", List.of(), null), "tenant_not_allowed");
  }

  @Test
  void mintsSpecClaimsWithTheRequestedTtl() throws Exception {
    var t = mint.mint(t1, "t1:u1", "d1", List.of("t1:news.*"), 120);
    assertThat(t.expiresAt()).isEqualTo(now.plusSeconds(120));
    var c = SignedJWT.parse(t.token()).getJWTClaimsSet();
    assertThat(c.getAudience()).containsExactly("pangate-gw");
    assertThat(c.getSubject()).isEqualTo("t1:u1");
    assertThat(c.getStringClaim("tenant")).isEqualTo("t1");
    assertThat(c.getStringClaim("device")).isEqualTo("d1");
    assertThat(c.getStringListClaim("topics")).containsExactly("t1:news.*");
    assertThat(c.getJWTID()).isNotBlank();
    assertThat(c.getNotBeforeTime().toInstant()).isEqualTo(now);
    assertThat(mint.mint(t1, "t1:u1", "d1", null, null).expiresAt()).isEqualTo(now.plusSeconds(600));
  }
}
