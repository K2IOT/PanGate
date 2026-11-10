package com.pangate.auth.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import com.nimbusds.jwt.SignedJWT;
import com.pangate.auth.config.AuthProperties;
import com.pangate.auth.domain.Scope;
import com.pangate.auth.domain.Tenant;
import com.pangate.auth.infrastructure.crypto.Es256TokenSigner;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.EnumSet;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IssueClientTokenTest {

  static final String ISS = "https://auth.pangate";
  private final Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
  private final InMemoryStores.Tenants tenants = new InMemoryStores.Tenants();
  private final InMemoryStores.Clients clients = new InMemoryStores.Clients();
  private final FakeHasher hasher = new FakeHasher();
  private final ManageClients manage = new ManageClients(clients, tenants, hasher);
  private final InMemorySigningKeyStore keys = new InMemorySigningKeyStore();
  private final KeyRing ring = new KeyRing(keys);
  private IssueClientToken issue;
  private String secret;

  @BeforeEach
  void setUp() {
    new RotateSigningKeys(keys, () -> now, RotateSigningKeysTest.KEYS).runOnce();
    ring.reload();
    new ManageTenants(tenants).create("t1");
    secret = manage.create("t1", "backend", EnumSet.of(Scope.TOKENS_MINT, Scope.PUSH_SEND)).plainSecret();
    issue = new IssueClientToken(clients, tenants, hasher, new Es256TokenSigner(ISS), ring, () -> now, ISS,
        Duration.ofMinutes(15));
  }

  private OAuthError fail(String clientId, String secret, String scope) {
    OAuthError e = catchThrowableOfType(OAuthError.class, () -> issue.issue(clientId, secret.toCharArray(), scope));
    assertThat(e).isNotNull();
    return e;
  }

  @Test
  void emptyScopeGrantsAllAssignedScopes() {
    assertThat(issue.issue("t1-backend", secret.toCharArray(), null).scope()).isEqualTo("tokens:mint push:send");
    assertThat(issue.issue("t1-backend", secret.toCharArray(), " ").scope()).isEqualTo("tokens:mint push:send");
    assertThat(issue.issue("t1-backend", secret.toCharArray(), "push:send").scope()).isEqualTo("push:send");
  }

  @Test
  void scopeOutsideTheAssignmentIsInvalidScope() {
    var e = fail("t1-backend", secret, "push:send sessions:revoke");
    assertThat(e.error()).isEqualTo("invalid_scope");
    assertThat(e.status()).isEqualTo(400);
    assertThat(fail("t1-backend", secret, "bogus").error()).isEqualTo("invalid_scope");
  }

  @Test
  void unknownClientBurnsOneHashAndFails() {
    var e = fail("t1-nobody", "x", null);
    assertThat(e.error()).isEqualTo("invalid_client");
    assertThat(e.status()).isEqualTo(401);
    assertThat(hasher.burns).hasValue(1);
    assertThat(fail("t1-backend", "wrong", null).error()).isEqualTo("invalid_client");
  }

  @Test
  void disabledClientOrTenantIsInvalidClient() {
    tenants.setActive("t1", false);
    assertThat(fail("t1-backend", secret, null).error()).isEqualTo("invalid_client");
    tenants.setActive("t1", true);
    manage.disable("t1-backend");
    assertThat(fail("t1-backend", secret, null).error()).isEqualTo("invalid_client");
  }

  @Test
  void rotatedSecretWorksToo() {
    String next = manage.rotateSecret("t1-backend");
    assertThat(issue.issue("t1-backend", next.toCharArray(), null).accessToken()).isNotBlank();
    assertThat(issue.issue("t1-backend", secret.toCharArray(), null).accessToken()).isNotBlank();
  }

  @Test
  void adminScopeIsNeverIssuedToACustomerTenant() {
    // a row that somehow holds admin (e.g. edited in the database) still gets no admin token
    var c = clients.rows.get("t1-backend");
    clients.update(new com.pangate.auth.domain.Client(c.id(), c.tenantId(), c.secretHash(), null,
        EnumSet.of(Scope.ADMIN, Scope.PUSH_SEND), true));
    assertThat(fail("t1-backend", secret, "admin").error()).isEqualTo("invalid_scope");
    assertThat(issue.issue("t1-backend", secret.toCharArray(), null).scope()).isEqualTo("push:send");
    assertThat(Tenant.isOperator("t1")).isFalse();
  }

  @Test
  void tokenCarriesSpecClaims() throws Exception {
    var t = issue.issue("t1-backend", secret.toCharArray(), null);
    assertThat(t.expiresIn()).isEqualTo(900);
    var jwt = SignedJWT.parse(t.accessToken());
    assertThat(jwt.getHeader().getAlgorithm().getName()).isEqualTo("ES256");
    assertThat(jwt.getHeader().getKeyID()).isEqualTo(ring.active().kid());
    var c = jwt.getJWTClaimsSet();
    assertThat(c.getIssuer()).isEqualTo(ISS);
    assertThat(c.getAudience()).isEqualTo(List.of("pangate-api"));
    assertThat(c.getSubject()).isEqualTo("client:t1-backend");
    assertThat(c.getStringClaim("tenant")).isEqualTo("t1");
    assertThat(c.getStringClaim("client_id")).isEqualTo("t1-backend");
    assertThat(c.getStringClaim("scope")).isEqualTo("tokens:mint push:send");
    assertThat(c.getJWTID()).isNotBlank();
    assertThat(c.getIssueTime().toInstant()).isEqualTo(now);
    assertThat(c.getNotBeforeTime().toInstant()).isEqualTo(now);
    assertThat(c.getExpirationTime().toInstant()).isEqualTo(now.plusSeconds(900));
    assertThat(new Es256TokenSigner(ISS).verifyApi(t.accessToken(), ring::publicKey, now)).isPresent();
  }
}
