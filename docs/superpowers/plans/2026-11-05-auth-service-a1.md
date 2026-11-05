# auth-service A1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `auth-service` (client credentials tokens, connect-token minting, JWKS with publish-ahead key rotation, revocations through an outbox to `gw.control`, internal Admin API) and switch the devenv from `devtools issuer` to it.

**Architecture:** A new independent Maven project `auth-service/` (Spring Boot MVC on virtual threads, hexagonal packages `api / application / domain / infrastructure`), PostgreSQL via `JdbcClient` + Flyway, Kafka via plain `kafka-clients`, ES256 via Nimbus. Small additive changes in `platform-contracts`, `gateway-service`, `gateway-devtools`, `platform-devenv`, `Makefile` and CI.

**Tech Stack:** Java 21, Spring Boot (version from `platform-parent`), Nimbus JOSE JWT 10.10, BouncyCastle (Argon2 via `spring-security-crypto`), PostgreSQL 16, Flyway, Kafka clients, Testcontainers, Jib.

**Spec:** `docs/superpowers/specs/2026-11-05-auth-service-design.md` (sections cited as "spec §N").

## Global Constraints

- Every module builds on its own: `auth-service/pom.xml` has parent `platform-parent` 0.1.0 (`relativePath ../platform-parent/pom.xml`); no SNAPSHOT versions; artifact version `0.1.0`.
- Package root `com.pangate.auth`; `domain/` and `application/` never import Spring, JDBC or Kafka types.
- Ports: public `8090`, management `8081` (spec §1).
- Connect token: header `ES256` + `kid`; claims `iss`, `aud=pangate-gw`, `sub`, `tenant`, `device`, `topics`, `jti`, `iat`, `nbf`, `exp` (spec §4.3).
- API token: `aud=pangate-api`, `sub=client:<id>`, `tenant`, `client_id`, `scope`, `jti`, 900 s (spec §4.1).
- Claim limits: tenant ≤ 64 B, principal ≤ 256 B with prefix `<tenant>:`, device 1..128 B, ≤ 8 topic patterns each ≤ 64 B with prefix `<tenant>:`, connect `ttlSec` in [60, 600] default 600 (spec §4.3). Byte lengths are UTF-8 bytes.
- Key lifetimes: `lifetime 30d`, `overlap 7d`, `publish-ahead 1h`, `cache-refresh 60s` (spec §8).
- Revocation TTL 24 h; control keys `revoke:p:<principal>`, `revoke:d:<principal>/<device>`, `revoke:j:<jti>`, `revoke:c:<client_id>` (spec §4.4, §4.6).
- Never log tokens, secrets, request bodies or private keys; hash principals with the starter's `PrincipalHasher` (spec §7).
- Tenant `__warmup` is rejected everywhere; tenant `pangate` is the operator tenant and the only one whose clients may hold scope `admin` (spec §4.1).
- Tests: unit in `src/test/java` (surefire), integration in `src/it/java` (failsafe, `*IT`), same build-helper layout as `gateway-service/pom.xml`.

## Review Focus

1. Multi-byte UTF-8 in `principal` / `topics`: limits are bytes, not chars; a 129-char ASCII principal plus a 2-byte char at the boundary must be judged by bytes (Task 3).
2. HTTP Basic credentials percent-encoded per RFC 6749 §2.3.1 (`client_id` with `-`, secrets are base64url but must still be URL-decoded) must authenticate (Task 7).
3. A revocation accepted while Kafka is down must still reach `gw.control` after the Auth process restarts (outbox survives restarts) (Task 9).
4. Re-revoking a still-valid key extends it: one `revocations` row, a new outbox record, later expiry (Task 9).
5. A second Auth pod that did not rotate picks up the new `pending` key in its JWKS within `cache-refresh` (Task 5).

---

### Task 1: Contract and gateway preparation

**Files:**
- Modify: `platform-contracts/proto/pangate/v1/control.proto` (add `string client_id = 5;` to `Revoke.oneof target`; add `revoke:c:<client_id>  Revoke{client_id}` to the key table comment)
- Modify: `gateway-service/src/main/java/com/pangate/gateway/control/ControlKeys.java`, `ControlEventConsumer.java`
- Modify: `gateway-service/pom.xml` (extra `maven-jar-plugin` execution, classifier `verifier`, `<includes>com/pangate/gateway/auth/**</includes>`)
- Test: `gateway-service/src/test/java/com/pangate/gateway/control/ControlKeysTest.java`, `ControlEventConsumerTest.java`

**Interfaces:**
- Produces: `ControlKeys.ClientRevokeKey(String clientId) implements Parsed`; artifact `com.pangate:gateway-service:0.1.0:jar:verifier`.

- [ ] **Step 1: Write the failing tests**

```java
@Test void clientRevokeKeyIsRecognised() {
  assertThat(ControlKeys.parse("revoke:c:t1-backend")).isEqualTo(new ControlKeys.ClientRevokeKey("t1-backend"));
  assertThat(ControlKeys.parse("revoke:c:")).isInstanceOf(ControlKeys.Unknown.class);
}
// ControlEventConsumerTest
@Test void clientRevokeIsIgnoredWithoutCountingABadRecord() {
  // feed key "revoke:c:t1-backend" with a Revoke{client_id} value
  // assert: no session closed, revocation list unchanged, metric gw.control.records{result=bad} == 0
}
```

- [ ] **Step 2: Run** `./mvnw -q -f platform-contracts install && ./mvnw -f gateway-service test -Dtest='ControlKeysTest,ControlEventConsumerTest'` — Expected: FAIL (`ClientRevokeKey` does not exist).
- [ ] **Step 3: Implement** `ClientRevokeKey` in `ControlKeys.parse` (prefix `revoke:c:`, non-empty id) and a no-op `case ControlKeys.ClientRevokeKey c -> { }` in `ControlEventConsumer` (applies to both values and tombstones). Add the `verifier` jar execution.
- [ ] **Step 4: Verify** the two tests pass; `buf lint` and `buf breaking --against '.git#branch=main,subdir=platform-contracts'` pass (or `make release-check` + contracts-lint locally via `buf` if installed); `./mvnw -f gateway-service install -DskipTests && unzip -l gateway-service/target/gateway-service-0.1.0-verifier.jar | grep -c 'com/pangate/gateway/auth/'` prints > 0 and no other package.
- [ ] **Step 5: Commit** `feat(contracts,gateway): Revoke.client_id; gateway ignores revoke:c: keys; verifier jar`

---

### Task 2: auth-service skeleton, schema and profiles

**Files:**
- Create: `auth-service/pom.xml` (deps: `platform-contracts`, `platform-spring-starter`, `spring-boot-starter-web`, `spring-boot-starter-actuator`, `spring-boot-starter-jdbc`, `flyway-core`, `flyway-database-postgresql`, `postgresql`, `kafka-clients`, `nimbus-jose-jwt`, `spring-security-crypto`, `bcprov-jdk18on`, `micrometer-registry-prometheus`; test: `spring-boot-starter-test`, `testcontainers` `postgresql` + `kafka` + `junit-jupiter`, `gateway-service:verifier` (test scope); plugins: build-helper (`src/it/java`), surefire, failsafe, `spring-boot-maven-plugin` repackage with classifier `exec`, Jib image `pangate/auth-service:dev`, `distroless java21`, ports 8090 8081)
- Create: `auth-service/src/main/java/com/pangate/auth/AuthApplication.java`, `config/AuthProperties.java`
- Create: `auth-service/src/main/resources/application.yml`, `application-local.yml`, `application-dev.yml`, `db/migration/V1__init.sql` (exactly spec §2)
- Modify: `Makefile` (`MODULES` append `auth-service` after `gateway-service`), `platform-devenv/scripts/release-check.sh` (version check covers `auth-service/pom.xml`)
- Test: `auth-service/src/it/java/com/pangate/auth/SchemaIT.java`; `auth-service/src/it/java/com/pangate/auth/support/Containers.java`

**Interfaces:**
- Produces: `AuthProperties` record bound to prefix `auth` with fields `issuer`, `apiTokenTtl`, `connectTokenMaxTtl`, `revocationTtl`, `keys(lifetime, overlap, publishAhead, kekId, cacheRefresh)`, `outbox(interval, batch, keepSent)`, `kafka(controlTopic, bootstrapServers)`, `seed(enabled, dir)` with the defaults of spec §8; `Containers.postgres()` / `Containers.kafka()` shared singletons; `@AuthIT` meta-annotation (`@SpringBootTest(webEnvironment = RANDOM_PORT)` + dynamic properties from `Containers`).
- `server.port=8090`, `management.server.port=8081`, `spring.threads.virtual.enabled=true`, `spring.flyway.schemas=auth`, `management.endpoint.health.group.readiness.include=readinessState,db`; `/ready` and `/live` mapped like `gateway-service` does.

- [ ] **Step 1: Write the failing test** `SchemaIT.migrationCreatesTablesAndConstraints`: after context start, `information_schema.tables` in schema `auth` contains `tenants, clients, signing_keys, revocations, outbox`; inserting tenant `__warmup` fails with a check violation; inserting two `active` signing keys fails on `one_active_key`.
- [ ] **Step 2: Run** `./mvnw -f auth-service verify -Dit.test=SchemaIT` — Expected: FAIL (no project / no migration).
- [ ] **Step 3: Implement** the pom, application class, properties, YAML profiles (`local`: datasource `jdbc:postgresql://localhost:5432/pangate`, user/password `pangate`, kafka `localhost:9092`, `auth.issuer=http://localhost:8090`, seed on, seed dir `.dev/auth` (relative to the repo root, where `make auth-local` runs); `dev`: host `postgres`, kafka `kafka:29092`, `auth.issuer=http://auth:8090`, seed on, seed dir `/dev-secrets/auth`), and `V1__init.sql`.
- [ ] **Step 4: Verify** `SchemaIT` passes; `make release-check` prints `release-check OK 0.1.0`.
- [ ] **Step 5: Commit** `feat(auth): service skeleton, schema V1, profiles`

---

### Task 3: Domain rules (claims, scopes, tenants, clients)

**Files:**
- Create: `domain/Scope.java`, `domain/ClaimLimits.java`, `domain/ConnectTokenRequest.java`, `domain/Tenant.java`, `domain/Client.java`, `domain/DomainException.java`
- Test: `auth-service/src/test/java/com/pangate/auth/domain/ClaimLimitsTest.java`, `ScopeTest.java`, `ClientTest.java`

**Interfaces:**
- Produces:
  - `enum Scope { TOKENS_MINT("tokens:mint"), SESSIONS_REVOKE("sessions:revoke"), PUSH_SEND("push:send"), PUSH_BROADCAST("push:broadcast"), PRESENCE_READ("presence:read"), DEVICES_WRITE("devices:write"), ADMIN("admin"); static Set<Scope> parse(String spaceSeparated); static String format(Set<Scope>) }`
  - `record ConnectTokenRequest(String tenant, String principal, String device, List<String> topics, int ttlSec)`
  - `final class ClaimLimits { static ConnectTokenRequest validate(String tenant, String principal, String device, List<String> topics, Integer ttlSec) }` — throws `DomainException(String code, int httpStatus)` with the codes of spec §4.3
  - `record Tenant(String id, boolean active)`, `static boolean Tenant.isOperator(String id)` (`"pangate"`), `static final String WARMUP = "__warmup"`
  - `record Client(String id, String tenantId, String secretHash, String secretHashNext, Set<Scope> scopes, boolean active)`; `static void Client.checkScopes(String tenantId, Set<Scope> scopes)` (throws `scope_not_allowed` if `ADMIN` for a non-operator tenant)

- [ ] **Step 1: Write the failing tests**

```java
@Test void principalMustBeInsideTenant() { assertCode(() -> validate("t1", "t2:u", "d", List.of(), null), "principal_outside_tenant"); }
@Test void principalLimitIsUtf8Bytes() {
  String ok = "t1:" + "a".repeat(253);                 // 256 bytes
  String tooLong = "t1:" + "a".repeat(252) + "é";      // 257 bytes, 256 chars
  assertThat(validate("t1", ok, "d", List.of(), null).principal()).isEqualTo(ok);
  assertCode(() -> validate("t1", tooLong, "d", List.of(), null), "principal_too_long");
}
@Test void deviceRequiredAndAtMost128Bytes() { /* "" -> device_invalid; 129 bytes -> device_invalid; 128 -> ok */ }
@Test void topics() { /* 9 patterns -> too_many_topics; "t2:x" -> topic_outside_tenant; "t1:" + 62 chars (65 B) -> topic_pattern_too_long; null -> List.of() */ }
@Test void ttl() { /* null -> 600; 59 and 601 -> ttl_out_of_range; 60 and 600 ok */ }
@Test void warmupAndOperatorTenantsCannotMint() { /* "__warmup" and "pangate" -> tenant_not_allowed (403) */ }
@Test void scopeParseRejectsUnknownAndFormatsSorted() { /* "push:send bogus" -> invalid_scope; format is space-separated in enum order */ }
@Test void adminScopeOnlyForOperatorTenant() { /* checkScopes("t1", {ADMIN}) -> scope_not_allowed; ("pangate", {ADMIN}) ok */ }
```

- [ ] **Step 2: Run** `./mvnw -f auth-service test -Dtest='ClaimLimitsTest,ScopeTest,ClientTest'` — Expected: FAIL (types missing).
- [ ] **Step 3: Implement** the types; tenant claim length (≤ 64 B, no `:`) is checked too, code `tenant_invalid`.
- [ ] **Step 4: Verify** the tests pass.
- [ ] **Step 5: Commit** `feat(auth): claim limits, scopes, tenant and client rules`

---

### Task 4: Crypto adapters and the gateway contract test

**Files:**
- Create: `application/port/KeyCipher.java`, `application/port/SecretHasher.java`, `application/port/TokenSigner.java`, `domain/SigningKey.java`, `domain/KeyState.java`, `domain/ApiClaims.java`, `domain/ConnectClaims.java`
- Create: `infrastructure/crypto/AesGcmKeyCipher.java`, `infrastructure/crypto/Argon2SecretHasher.java`, `infrastructure/crypto/Es256TokenSigner.java`, `infrastructure/crypto/Kek.java`
- Test: `src/test/java/com/pangate/auth/infrastructure/crypto/AesGcmKeyCipherTest.java`, `Argon2SecretHasherTest.java`, `GatewayContractTest.java`

**Interfaces:**
- Produces:
  - `interface KeyCipher { byte[] encrypt(String kid, byte[] plain); byte[] decrypt(String kekId, String kid, byte[] sealed); String currentKekId(); }` — AES-256-GCM, 12 B random nonce, AAD = `kid` UTF-8, sealed layout `nonce || ciphertext || tag`
  - `Kek.load(SecretSource, String kekId)` reads secret `auth.kek.<kekId>`, requires exactly 32 bytes (else `IllegalStateException` naming the secret)
  - `interface SecretHasher { String hash(char[] secret); boolean matches(char[] secret, String hash); void burn(); }` — `Argon2PasswordEncoder(16, 32, 1, 19456, 2)`; `burn()` runs one verify against a fixed dummy hash (timing equalisation, spec §4.1)
  - `record SigningKey(String kid, ECKey privateJwk, KeyState state, Instant activateAt, Instant retireAt, Instant removeAt)`; `enum KeyState { PENDING, ACTIVE, RETIRED }`
  - `record ApiClaims(String issuer, String clientId, String tenant, Set<Scope> scopes, String jti, Instant issuedAt, Instant expiresAt)`; `record ConnectClaims(String issuer, ConnectTokenRequest req, String jti, Instant issuedAt, Instant expiresAt)`
  - `interface TokenSigner { String sign(SigningKey key, ApiClaims c); String sign(SigningKey key, ConnectClaims c); Optional<ApiClaims> verifyApi(String token, Function<String, Optional<ECPublicKey>> keyByKid, Instant now); }` — `verifyApi` checks the signature with the key the lookup returns (Task 5 passes `KeyRing::publicKey`), `aud=pangate-api`, `iss`, `exp/nbf` with 30 s skew; any failure → `Optional.empty()`
  - `static SigningKey Es256TokenSigner.newKey(Instant activateAt, KeyState state)` — `kid` = RFC 7638 thumbprint

- [ ] **Step 1: Write the failing tests**

```java
// AesGcmKeyCipherTest
@Test void roundTrip() ; @Test void wrongKekFails() ; @Test void kidIsBoundAsAad() /* decrypt with another kid throws */ ;
@Test void kekMustBe32Bytes() /* 31-byte secret -> IllegalStateException mentioning "auth.kek.v1" */
// Argon2SecretHasherTest
@Test void hashMatchesOnlyTheSameSecret() ; @Test void hashEncodesArgon2id() /* startsWith("$argon2id$") */
// GatewayContractTest — uses com.pangate.gateway.auth.{JwksCache, JwksSource, JwtVerifier, JwtResult} from the verifier jar
@Test void gatewayAcceptsAConnectTokenMintedByAuth() {
  // key = newKey(now, ACTIVE); JwksSource returning Updated(new JWKSet(key.toPublicJWK()), "e1", 5m)
  // verifier = new JwtVerifier(cache, "pangate-gw", "https://auth.pangate", Duration.ofSeconds(30))
  // token = signer.sign(key, connect claims for t1:u1 / d1 / ["t1:news.*"], 600 s)
  // assert verify(token, now) is JwtResult.Verified with tenant t1, principal t1:u1, device d1, topics ["t1:news.*"]
}
@Test void gatewayRejectsAnApiToken() { /* signer.sign(key, apiClaims) -> JwtResult.Rejected("wrong audience") */ }
```

- [ ] **Step 2: Run** `./mvnw -f auth-service test -Dtest='AesGcmKeyCipherTest,Argon2SecretHasherTest,GatewayContractTest'` — Expected: FAIL.
- [ ] **Step 3: Implement** the adapters (Nimbus `ECDSASigner`/`ECDSAVerifier`, `JWSHeader.Builder(ES256).keyID(kid)`).
- [ ] **Step 4: Verify** the tests pass.
- [ ] **Step 5: Commit** `feat(auth): AES-GCM key cipher, Argon2id hasher, ES256 signer; gateway contract test`

---

### Task 5: Signing key store, rotation and JWKS

**Files:**
- Create: `application/port/SigningKeyStore.java`, `application/port/Clock.java`, `application/RotateSigningKeys.java`, `application/KeyRing.java`
- Create: `infrastructure/postgres/JdbcSigningKeyStore.java`, `infrastructure/postgres/AdvisoryLock.java`, `api/JwksController.java`, `config/JobsConfig.java`
- Test: `src/test/java/com/pangate/auth/application/RotateSigningKeysTest.java` (in-memory store, fake clock), `src/it/java/com/pangate/auth/KeyRotationIT.java`

**Interfaces:**
- Consumes: `KeyCipher`, `SigningKey`, `KeyState`, `Es256TokenSigner.newKey` (Task 4).
- Produces:
  - `interface SigningKeyStore { List<SigningKey> all(); void insert(SigningKey k); void update(SigningKey k); void delete(String kid); }` (decrypts via `KeyCipher`; undecryptable rows are returned as absent and counted in `auth.keys.undecryptable`)
  - `interface Clock { Instant now(); }`
  - `final class RotateSigningKeys { RotateSigningKeys(SigningKeyStore, Clock, AuthProperties.Keys); boolean runOnce(); }` — the four rules of spec §3, returns true if anything changed
  - `final class KeyRing { void reload(); SigningKey active(); Optional<ECPublicKey> publicKey(String kid); String jwksJson(); }` — reload after a rotation on this pod, and every `cache-refresh` on all pods
  - `interface AdvisoryLock { boolean runExclusive(String name, Runnable task); }` (`pg_try_advisory_xact_lock(hashtext(name))` in a transaction)
  - `GET /.well-known/jwks.json` → `KeyRing.jwksJson()`, `Cache-Control: public, max-age=300`

- [ ] **Step 1: Write the failing tests**

```java
// RotateSigningKeysTest (clock starts at T0)
@Test void firstRunCreatesAndActivatesAKeyAtOnce() ;           // one ACTIVE, activateAt == T0
@Test void pendingKeyAppearsOneDayBeforeExpiry() ;            // at T0+29d: one PENDING, activateAt = now + 1h
@Test void pendingIsActivatedAfterPublishAhead() ;            // at T0+29d+1h: old RETIRED (removeAt = now+7d), new ACTIVE
@Test void retiredKeyIsDeletedAfterOverlap() ;                // at retire+7d: deleted
@Test void noChangeWhenNothingIsDue() ;                       // runOnce() == false
// KeyRotationIT (Postgres container, two Spring contexts on the same DB)
@Test void onlyOneContextRotatesAtATime() ;                   // both call runOnce concurrently: exactly one ACTIVE row, no exception escapes
@Test void otherPodSeesPendingKeyWithinCacheRefresh() ;      // Review Focus 5: cache-refresh 1s in this test
@Test void jwksListsPendingActiveAndRetired() ;
@Test void tokenSignedWithOldKeyVerifiesThroughTheOverlap() ;  // spec §12 item 5, with the fake clock
```

- [ ] **Step 2: Run** `./mvnw -f auth-service verify -Dtest=RotateSigningKeysTest -Dit.test=KeyRotationIT` — Expected: FAIL.
- [ ] **Step 3: Implement** store, rotation, key ring, controller; schedule `RotateSigningKeys` every 1 h and once at startup under lock `auth.rotate-keys`, and `KeyRing.reload` every `cache-refresh`.
- [ ] **Step 4: Verify** both pass.
- [ ] **Step 5: Commit** `feat(auth): signing keys with publish-ahead rotation and JWKS`

---

### Task 6: Tenants, clients, Admin API, API authentication and seed

**Files:**
- Create: `application/port/TenantStore.java`, `application/port/ClientStore.java`, `application/ManageTenants.java`, `application/ManageClients.java`
- Create: `infrastructure/postgres/JdbcTenantStore.java`, `JdbcClientStore.java`, `api/ApiAuthFilter.java`, `api/ApiPrincipal.java`, `api/AdminController.java`, `api/ProblemHandler.java`, `config/SeedRunner.java`, `config/DevSecrets.java`
- Test: `src/test/java/com/pangate/auth/application/ManageClientsTest.java`, `src/it/java/com/pangate/auth/AdminApiIT.java`, `SeedIT.java`

**Interfaces:**
- Consumes: `SecretHasher`, `TokenSigner.verifyApi`, `KeyRing` (Tasks 4–5), `Scope`, `Client`, `Tenant` (Task 3).
- Produces:
  - `ManageClients.create(String tenantId, String name, Set<Scope>) -> CreatedClient(String clientId, String plainSecret)`; `rotateSecret(String clientId) -> String`; `finishRotation(String clientId)`; `disable(String clientId)` (the revocation side effect is added in Task 9 through `Revoke.client(...)`)
  - `ManageTenants.create(String id)`, `disable(String id)`, `enable(String id)`, `list()`, `get(String id)`
  - `record ApiPrincipal(String clientId, String tenant, Set<Scope> scopes, String jti)` set as request attribute `ApiPrincipal.ATTR`; `ApiAuthFilter` applies to `/v1/**` and `/admin/v1/**`; disabled clients and revoked jtis come from a 10 s cache (`ClientStore.disabledIds()`, `RevocationStore.activeJtis()` — the latter stubbed empty until Task 9)
  - `ProblemHandler` maps `DomainException(code, status)` to RFC 9457 `application/problem+json` with `code`
  - `SeedRunner` (when `auth.seed.enabled`): idempotently creates tenant `pangate` + client `pangate-admin` `{ADMIN}`, tenant `t1` + client `t1-backend` `{TOKENS_MINT, SESSIONS_REVOKE, PUSH_SEND, PUSH_BROADCAST}`; writes `<seed.dir>/clients.json` `{"pangate-admin": "<secret>", "t1-backend": "<secret>"}` once (existing file wins: secrets are then read from it and must match)
  - `DevSecrets` (profiles `local`, `dev` only): creates `<seed.dir>/kek-v1.bin` (32 random bytes, mode 600) if missing and points `pangate.secrets.dir` so `auth.kek.v1` resolves to it

Admin routes exactly as spec §4.5 (Task 9 adds the revocation in `:disable`; `/keys:rotate-now` calls a `RotateSigningKeys.forcePending()` method added here).

- [ ] **Step 1: Write the failing tests**

```java
// ManageClientsTest
@Test void createReturnsSecretOnceAndStoresOnlyAHash() ;
@Test void rotationAcceptsBothSecretsUntilFinished() ;
@Test void adminScopeRefusedForCustomerTenant() ;          // code scope_not_allowed
// AdminApiIT
@Test void adminRoutesNeedAdminScope() ;                    // t1-backend token -> 403; pangate-admin token -> 201
@Test void adminRoutesAreNotOnThePublicPort() ;             // POST :8090/admin/v1/tenants -> 404
@Test void listNeverReturnsSecretsOrHashes() ;             // response body contains neither "secret" nor "$argon2"
@Test void unknownTenantIs404ProblemJson() ;               // content-type application/problem+json, code tenant_not_found
// SeedIT
@Test void seedIsIdempotentAndWritesClientsFileOnce() ;
```

- [ ] **Step 2: Run** `./mvnw -f auth-service verify -Dtest=ManageClientsTest -Dit.test='AdminApiIT,SeedIT'` — Expected: FAIL.
- [ ] **Step 3: Implement.** Admin tokens in `AdminApiIT` are obtained by signing `ApiClaims` directly with the active key (the token endpoint arrives in Task 7).
- [ ] **Step 4: Verify** all pass.
- [ ] **Step 5: Commit** `feat(auth): tenants, clients, Admin API, bearer authentication, dev seed`

---

### Task 7: Client credentials endpoint

**Files:**
- Create: `application/IssueClientToken.java`, `api/TokenEndpoint.java`, `api/OAuthError.java`
- Test: `src/test/java/com/pangate/auth/application/IssueClientTokenTest.java`, `src/it/java/com/pangate/auth/ClientCredentialsIT.java`

**Interfaces:**
- Consumes: `ClientStore`, `TenantStore`, `SecretHasher`, `TokenSigner`, `KeyRing`, `Clock`.
- Produces: `IssueClientToken.issue(String clientId, char[] secret, String requestedScope) -> IssuedToken(String accessToken, long expiresIn, String scope)`; failures as `OAuthError(String error, int status)` with `invalid_client` (401), `invalid_scope` (400); `TokenEndpoint` maps `invalid_request` (400, both Basic and form credentials, or neither) and `unsupported_grant_type` (400). Responses carry `Cache-Control: no-store`; 401 carries `WWW-Authenticate: Basic realm="pangate"`.

- [ ] **Step 1: Write the failing tests**

```java
// IssueClientTokenTest
@Test void emptyScopeGrantsAllAssignedScopes() ;
@Test void scopeOutsideTheAssignmentIsInvalidScope() ;
@Test void unknownClientBurnsOneHashAndFails() ;          // SecretHasher.burn() called once, error invalid_client
@Test void disabledClientOrTenantIsInvalidClient() ;
@Test void tokenCarriesSpecClaims() ;                     // aud pangate-api, sub client:t1-backend, tenant, client_id, scope, jti, exp = iat + 900
// ClientCredentialsIT
@Test void basicAuthWithPercentEncodedCredentials() ;     // Review Focus 2
@Test void formCredentialsWork() ;
@Test void bothCredentialStylesIsInvalidRequest() ;
@Test void wrongGrantTypeIsUnsupportedGrantType() ;
```

- [ ] **Step 2: Run** `./mvnw -f auth-service verify -Dtest=IssueClientTokenTest -Dit.test=ClientCredentialsIT` — Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify** pass.
- [ ] **Step 5: Commit** `feat(auth): OAuth2 client credentials endpoint`

---

### Task 8: Connect token minting

**Files:**
- Create: `application/MintConnectToken.java`, `api/ConnectTokenController.java`
- Test: `src/test/java/com/pangate/auth/application/MintConnectTokenTest.java`, `src/it/java/com/pangate/auth/ConnectTokenIT.java`

**Interfaces:**
- Consumes: `ClaimLimits.validate`, `TokenSigner.sign(SigningKey, ConnectClaims)`, `KeyRing.active()`, `TenantStore`, `ApiPrincipal`.
- Produces: `MintConnectToken.mint(ApiPrincipal caller, String principal, String device, List<String> topics, Integer ttlSec) -> MintedToken(String token, Instant expiresAt)`; `POST /v1/tokens` body `{principal, device, topics, ttlSec}` → `200 {token, expiresAt}`; needs `tokens:mint` (else 403 `missing_scope`); tenant from `caller.tenant()` only.

- [ ] **Step 1: Write the failing tests**

```java
// MintConnectTokenTest
@Test void tenantComesFromTheCallerNotTheBody() ;          // caller t1, principal "t2:x" -> principal_outside_tenant
@Test void disabledTenantIsForbidden() ;                   // tenant_disabled, 403
// ConnectTokenIT
@Test void mintedTokenPassesTheGatewayVerifier() ;         // fetch /.well-known/jwks.json over HTTP into a gateway JwksCache, verify with JwtVerifier
@Test void missingScopeIs403() ;
@Test void limitViolationsAre400ProblemJsonWithCode() ;    // one case per code of spec §4.3
```

- [ ] **Step 2: Run** `./mvnw -f auth-service verify -Dtest=MintConnectTokenTest -Dit.test=ConnectTokenIT` — Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify** pass.
- [ ] **Step 5: Commit** `feat(auth): POST /v1/tokens mints gateway connect tokens`

---

### Task 9: Revocations, outbox relay and expiry

**Files:**
- Create: `domain/Revocation.java`, `application/port/RevocationStore.java`, `application/port/Outbox.java`, `application/Revoke.java`, `application/ExpireRevocations.java`, `api/RevocationController.java`
- Create: `infrastructure/postgres/JdbcRevocationStore.java`, `JdbcOutbox.java`, `infrastructure/kafka/OutboxRelay.java`, `infrastructure/kafka/ControlEventMapper.java`
- Modify: `application/ManageClients.java` (`disable` revokes `client`), `api/ApiAuthFilter.java` (real `activeJtis()`)
- Test: `src/test/java/com/pangate/auth/infrastructure/kafka/ControlEventMapperTest.java`, `src/test/java/com/pangate/auth/application/RevokeTest.java`, `src/it/java/com/pangate/auth/RevocationIT.java`

**Interfaces:**
- Produces:
  - `record Revocation(long id, String tenantId, Kind kind, String controlKey, Instant expiresAt, String createdBy)`, `enum Kind { PRINCIPAL, DEVICE, JTI, CLIENT }`
  - `ControlEventMapper.key(Kind, String principal, String device, String jti, String clientId) -> String` (keys of the Global Constraints); `ControlEventMapper.value(Revocation, Instant issuedAt) -> byte[]` (`ControlEvent{revoke{...; expires_at_ms}, issued_at_ms}`)
  - `Revoke.principal(ApiPrincipal, String principal)`, `Revoke.device(ApiPrincipal, String principal, String device)`, `Revoke.jti(ApiPrincipal, String jti)`, `Revoke.client(String actor, String clientId)` → `Revocation`; each runs in one transaction: upsert `revocations` by `control_key` (extend `expires_at` if present) + insert `outbox`
  - `interface Outbox { void append(String topic, String key, byte[] payloadOrNull); List<OutboxRow> unsent(int limit); void markSent(long id); int deleteSentBefore(Instant t); }`
  - `OutboxRelay.runOnce()` (lock `auth.outbox-relay`, `FOR UPDATE SKIP LOCKED`, idempotent producer `acks=all`, headers via `PangateHeaders` with `tenant-id` and `content-type: application/x-protobuf; schema=pangate.v1.ControlEvent`; stops at the first send failure)
  - `ExpireRevocations.runOnce()` (lock `auth.expire-revocations`): tombstone + delete per expired row
  - `POST /v1/revocations` → `202 {id, controlKey, expiresAt}` (needs `sessions:revoke`)

- [ ] **Step 1: Write the failing tests**

```java
// ControlEventMapperTest
@Test void keysMatchTheGatewayContract() ;   // "revoke:p:t1:u1", "revoke:d:t1:u1/d1", "revoke:j:abc", "revoke:c:t1-backend"
@Test void valueRoundTripsThroughControlEvent() ;
// RevokeTest
@Test void principalOutsideCallerTenantIs403() ;
@Test void reRevokeExtendsTheSameRow() ;     // Review Focus 4
// RevocationIT (Postgres + Kafka containers)
@Test void revocationReachesGwControlWithKeyAndExpiry() ;
@Test void kafkaDownStillReturns202AndDeliversInOrderLater() ;   // pause Kafka container, 3 revocations, unpause: 3 records in id order
@Test void outboxSurvivesARestart() ;        // Review Focus 3: close the context while Kafka is paused, start a new one, records arrive
@Test void expiredRevocationIsTombstoned() ; // revocation-ttl 2s in this test
@Test void disablingAClientRevokesItsTokensAtOnce() ;  // ApiAuthFilter rejects its existing token within 10 s; revoke:c: record published
```

- [ ] **Step 2: Run** `./mvnw -f auth-service verify -Dtest='ControlEventMapperTest,RevokeTest' -Dit.test=RevocationIT` — Expected: FAIL.
- [ ] **Step 3: Implement**; schedule relay every `outbox.interval`, expiry every 60 s, cleanup of sent rows older than `keep-sent` daily.
- [ ] **Step 4: Verify** pass.
- [ ] **Step 5: Commit** `feat(auth): revocations through a transactional outbox to gw.control`

---

### Task 10: Rate limits, failure behaviour, diagnostics and metrics

**Files:**
- Create: `infrastructure/ratelimit/TokenBuckets.java`, `api/RateLimitFilter.java`, `diagnostics/KekFailureAnalyzer.java`, `src/main/resources/META-INF/spring.factories`, `infrastructure/AuthMetrics.java`
- Modify: controllers and jobs to record the metrics of spec §7
- Test: `src/test/java/com/pangate/auth/infrastructure/ratelimit/TokenBucketsTest.java`, `src/test/java/com/pangate/auth/diagnostics/KekFailureAnalyzerTest.java`, `src/it/java/com/pangate/auth/DependencyFailureIT.java`, `RateLimitIT.java`

**Interfaces:**
- Produces: `TokenBuckets.tryAcquire(String bucket, String key) -> OptionalLong retryAfterMs` with the limits of spec §7 (configurable under `auth.rate-limits.*`); failed-auth IP block (> 20/min → 60 s); body limit 16 KB (`413`); DB failures mapped to `503` + `Retry-After: 5` (`DataAccessException` in `ProblemHandler`; `/oauth2/token` uses OAuth error `temporarily_unavailable`).

- [ ] **Step 1: Write the failing tests**

```java
@Test void bucketRefillsAndReportsRetryAfter() ;
@Test void kekAnalyzerNamesSecretAndLocation() ;           // message contains "auth.kek.v1" and SecretSource.describe(...)
// DependencyFailureIT
@Test void postgresDownKeepsJwksAndFailsTokenWith503() ;   // mint a connect token, stop the container: JWKS 200, the earlier token still verifies with the gateway JwtVerifier fed from that JWKS, /oauth2/token 503 temporarily_unavailable, /ready 503, /live 200
@Test void kafkaDownKeepsReady200() ;
// RateLimitIT
@Test void tokenEndpointLimitsPerClient() ;                // 21 rapid calls -> at least one 429 with Retry-After
@Test void repeatedBadSecretsBlockTheIp() ;
@Test void oversizedBodyIs413() ;
```

- [ ] **Step 2: Run** `./mvnw -f auth-service verify -Dtest='TokenBucketsTest,KekFailureAnalyzerTest' -Dit.test='DependencyFailureIT,RateLimitIT'` — Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify** pass; whole module `./mvnw -f auth-service verify` green.
- [ ] **Step 5: Commit** `feat(auth): rate limits, dependency failure behaviour, diagnostics, metrics`

---

### Task 11: Devenv, devtools, gateway config and CI

**Files:**
- Modify: `platform-devenv/docker-compose.yml` (add `postgres` `postgres:16.4` with user/db/password `pangate`, healthcheck `pg_isready`, port 5432; add `auth` (profile `app`, image `pangate/auth-service:dev`, `SPRING_PROFILES_ACTIVE: dev`, volume `dev-secrets:/dev-secrets`, ports 8090, depends on `postgres` healthy + `kafka-init` completed, healthcheck on `http://127.0.0.1:8081/ready`); remove `issuer`; `gateway.depends_on` swaps `issuer` for `auth`)
- Modify: `Makefile` (`infra-up` starts `postgres` too; `images` builds auth; new `auth-local` = run `auth-service` exec jar with profile `local`; new `auth-local-check` = start it, wait for `/ready`, mint a connect token with `devtools token --auth-url`, stop; `smoke` passes `--auth-url http://auth:8090 --client-file /dev-secrets/auth/clients.json` instead of `--issuer`)
- Modify: `gateway-service/src/main/resources/application-dev.yml` (`jwks-uri: http://auth:8090/.well-known/jwks.json`, `issuer: http://auth:8090`), `application.yml` (`issuer: https://auth.pangate`)
- Modify: `gateway-devtools/.../cmd/SmokeCommand.java`, `TokenCommand.java`; Create: `gateway-devtools/.../auth/AuthClient.java`
- Modify: `.github/workflows/ci.yml` (`local-start` also runs `make auth-local-check`)
- Test: `gateway-devtools/src/test/java/com/pangate/devtools/auth/AuthClientTest.java`; end-to-end via `make images up smoke down`

**Interfaces:**
- Produces: `AuthClient(URI authUrl, Path clientFile, String clientId)` with `String apiToken()` (client credentials, cached until 60 s before expiry), `String connectToken(String principal, String device, List<String> topics)`, `void revokePrincipal(String principal)`. `SmokeCommand`: `--issuer` and `--auth-url` are mutually exclusive (exactly one required); with `--auth-url`, after the ACK step it revokes `t1:smoke` and expects the socket to close with 4003 within 10 s (new step `REVOKE`). `TokenCommand` gains `--auth-url`, `--client-file`, `--client` (default `t1-backend`) as an alternative to `--key`.

- [ ] **Step 1: Write the failing test** `AuthClientTest` against an in-process `HttpServer` stub: client credentials request is form-encoded with Basic auth; the API token is reused until 60 s before `expires_in`; `connectToken` posts JSON with the bearer token; non-2xx responses raise an exception carrying status and body.
- [ ] **Step 2: Run** `./mvnw -f gateway-devtools test -Dtest=AuthClientTest` — Expected: FAIL.
- [ ] **Step 3: Implement** devtools changes, compose, Makefile, gateway YAML, CI.
- [ ] **Step 4: Verify:**
  - `./mvnw -f gateway-devtools test` green.
  - `make verify` green (all modules, auth included).
  - `make images up smoke down` prints the smoke steps including `REVOKE` and `SMOKE OK`; gateway logs show JWKS fetched from `http://auth:8090`.
  - `make infra-up auth-local-check infra-down` green.
- [ ] **Step 5: Commit** `feat(devenv): auth-service replaces devtools issuer in dev; smoke covers revoke`

---

### Task 12: Docs, ADRs and release bookkeeping

**Files:**
- Create: `docs/adr/0013-auth-managed-signing-keys.md`, `0014-shared-keys-split-by-audience.md`, `0015-revocation-outbox.md` (headings `## Trạng thái`, `## Bối cảnh`, `## Quyết định`, `## Hệ quả`, `## Nguồn`, as `release-check.sh` requires)
- Create: `auth-service/README.md` (run modes, endpoints, admin API, seed files, metrics)
- Modify: `docs/adr/README.md` (index 0013–0015), `CHANGELOG.md` (`## Unreleased` → `auth-service` entry), root `README.md` (module list), `docs/superpowers/specs/2026-11-05-auth-service-design.md` (status line: implemented, with measured numbers)

- [ ] **Step 1:** Run `make release-check` before writing the ADRs — Expected: OK (baseline).
- [ ] **Step 2:** Write the docs; run `make release-check` — Expected: `release-check OK 0.1.0` with the three new ADRs indexed.
- [ ] **Step 3: Commit** `docs: auth-service README, ADR 0013-0015, changelog`

---

## Exit (spec §12) → tasks

| Exit criterion | Proven by |
|---|---|
| 1 Unit, contract, IT green; `buf breaking` green | Tasks 1–10 tests; Task 1 Step 4 |
| 2 Starts in `local` and `dev` | Task 11 (`auth-local-check`, `make up`) |
| 3 Gateway `dev` uses Auth JWKS only; `issuer` gone from compose `app` | Task 11 |
| 4 `make smoke` green incl. revoke (4003 < 10 s) | Task 11 |
| 5 Key rotation drill | Task 5 `tokenSignedWithOldKeyVerifiesThroughTheOverlap` |
| 6 Postgres down: JWKS 200, gateway still accepts HELLO with an earlier token | Task 10 `postgresDownKeepsJwksAndFailsTokenWith503` (JWKS + token verify by the gateway verifier) |
| 7 CI green | push after Task 12 |
