# account-service CH2 (Tài khoản) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Module mới `account-service`: đăng ký / đăng nhập bằng email, số điện thoại hoặc username + mật khẩu; phiên theo thiết bị với refresh token đổi lấy connect token của auth-service; hồ sơ và tìm theo username; admin khoá / mở khoá / đặt lại mật khẩu / xoá. chat-service biết user qua topic compacted `account.users`.

**Architecture:** Hexagonal như `auth-service`: `domain/` (Java thuần), `application/` + `port/` (use case), `infrastructure/{postgres,redis,kafka,authclient,crypto,auth}`, `api/` (controller, filter), `config/`. Thay đổi ghi PostgreSQL cùng outbox trong một transaction; hai relay độc lập đẩy `AccountState` lên Kafka và lệnh thu hồi sang auth-service.

**Tech Stack:** Java 21, Spring Boot 3.5.16 (MVC, JDBC, virtual threads), PostgreSQL 16 + Flyway, Lettuce (Redis), Kafka clients, Nimbus JOSE, Spring Security Crypto (`Argon2PasswordEncoder`) + BouncyCastle, Testcontainers 1.21.4 (`postgresql`, `kafka`, Redis `GenericContainer`), image `postgres:16.4`, `redis:7.4.2`.

**Spec:** `docs/superpowers/specs/2026-11-16-account-service-design.md`

## Global Constraints

- Không sửa gateway, router, Push API. auth-service chỉ thêm scope `accounts:admin` và seed client `chat-admin` (spec §9).
- Contracts chỉ thêm `platform-contracts/proto/pangate/v1/account.proto` (`AccountState`), package `pangate.v1`; `buf breaking` xanh (spec §6, §9).
- Tenant `chat`; principal `chat:<userId>`; `userId` và `deviceId` là ULID, không bao giờ cấp lại (spec §0, §2).
- Thu hồi theo `device` cho đăng xuất / đổi mật khẩu / đặt lại mật khẩu / khoá; chỉ xoá tài khoản thu hồi `principal` (spec §2, ADR 0036).
- Refresh token: 32 byte ngẫu nhiên, base64url, tiền tố `rt_`; chỉ lưu SHA-256; không xoay vòng; hết hạn khi không dùng 180 ngày (`account.session-idle-ttl`); `last_used_at` ghi tối đa mỗi 5 phút mỗi phiên (spec §2).
- Connect token: `POST /v1/tokens` với `principal=chat:<userId>`, `device=<deviceId>`, `topics=[]`, `ttlSec=600`, bằng token API của client `chat-backend` (spec §2).
- Quy tắc dữ liệu đúng bảng spec §4.1 (username `^[a-zA-Z][a-zA-Z0-9_]{4,31}$` không phân biệt hoa thường; email ≤ 254, chữ thường; phone `^\+[1-9][0-9]{6,14}$`; mật khẩu 8..128 ký tự NFC; displayName 1..64; bio ≤ 140).
- Argon2id m=19 MiB, t=2, p=1; tối đa `2 × số core` lần băm đồng thời mỗi pod, vượt → `503` + `Retry-After: 1` (spec §7).
- Rate limit đúng bảng spec §7 (login+IP 10/15 phút chặn 15 phút; IP 50/15 phút; login mọi IP 100/15 phút → trễ 2 s; đăng ký 5/giờ/IP; đổi token 60/giờ/phiên; tra user 600/phút/user; admin 20/s/client).
- Topic `account.users`: 12 partition, `cleanup.policy=compact`, key `userId`, không tombstone (spec §6).
- Lỗi HTTP: `application/problem+json` với `code` ổn định (các code trong spec §4.1, §4.2, §5).
- Không log mật khẩu, refresh token, connect token, body request; `userId` và login băm bằng `PrincipalHasher` của starter (spec §7).
- Cổng: `:8080` public, `:8081` admin + Actuator; devenv host `8088:8080`, `8089:8081` (spec §1, §9).
- Mỗi commit kết thúc bằng hai dòng trailer của session (Co-Authored-By, Claude-Session).

## Review Focus

1. **Email / username khác hoa thường hoặc có khoảng trắng** (`" Lan@Mail.com "`, `LanNguyen` vs `lannguyen`): đăng ký trùng → `*_taken`; đăng nhập bằng dạng khác vẫn vào đúng tài khoản. → Task 3 `LoginKeysTest.normalisationMakesOneKey`, Task 5 `RegisterLoginIT.loginIsCaseAndSpaceInsensitive`.
2. **Mật khẩu có ký tự Unicode tổ hợp** (`"é"` dạng NFC và NFD): hai cách gõ là cùng một mật khẩu. → Task 3 `PasswordPolicyTest.nfcAndNfdAreTheSamePassword`.
3. **Xoá rồi đăng ký lại cùng username / email ngay**: thành công, ra `userId` mới; `userId` cũ vẫn trả `deleted: true`. → Task 9 `AdminAndDeleteIT.deletedIdentifiersAreReusableAndOldIdStaysDeleted`.
4. **Hai yêu cầu đổi token đồng thời từ một máy, hoặc gọi lại sau khi đăng xuất thiết bị khác**: thiết bị hiện tại không bao giờ bị thu hồi oan. → Task 7 `SessionsIT.passwordChangeKeepsTheCurrentDeviceConnected`.
5. **Người dùng mới đăng ký ngay lập tức được thêm vào nhóm** (sự kiện chưa tới chat-service): `404 user_not_found`, gọi lại sau ≤ 2 s thì được. → Task 10 `AccountsListenerIT.newUserIsKnownWithinTwoSeconds`.

---

## File Structure

```
platform-contracts/proto/pangate/v1/account.proto                                   (Task 1)
platform-contracts-testkit/.../testkit/AccountTopics.java                           (Task 1)
auth-service/.../domain/Scope.java, config/SeedRunner.java                          (Task 1)
account-service/pom.xml, src/main/resources/{application.yml,application-local.yml,application-dev.yml,db/migration/V1__init.sql}  (Task 2)
account-service/src/main/java/com/pangate/account/
  AccountApplication.java
  config/          AccountProperties, AdminConnector, AccountConfig, JobsConfig, AuthClientSecret       (Task 2, mở rộng dần)
  domain/          Username, Email, Phone, Login, LoginKind, PasswordPolicy, ProfileRules, Account,
                   AccountStatus, Session, DomainException                                              (Task 3)
  application/     Register, Login, ExchangeToken, ManageSessions, Profiles, ChangePassword,
                   ChangeIdentifier, AdminAccounts, DeleteAccount                                        (Task 5–9)
  application/port/ AccountStore, SessionStore, Outbox, AdminLog, PasswordHasher, AuthApi, Clock,
                   Transactions, AttemptCounter                                                         (Task 4–6)
  infrastructure/postgres/  JdbcAccountStore, JdbcSessionStore, JdbcOutbox, JdbcAdminLog, AdvisoryLock  (Task 4)
  infrastructure/crypto/    Argon2PasswordHasher, HashGate, RefreshTokens                               (Task 3)
  infrastructure/redis/     RedisAttemptCounter (+ in-memory fallback)                                  (Task 5)
  infrastructure/authclient/ HttpAuthApi                                                                (Task 6)
  infrastructure/kafka/     EventRelay, AccountStateMapper                                              (Task 6)
  infrastructure/auth/      ConnectTokenVerifier, Revocations, RevocationListener, JwksSource,
                            HttpJwksSource, ApiTokenVerifier                                            (Task 7, 9)
  api/             Problems, ProblemHandler, PortRoutingFilter, ConnectAuthFilter, AdminAuthFilter,
                   ClientIp, AccountsController, SessionsController, MeController, UsersController,
                   AdminController                                                                      (Task 5–9)
chat-service/...  store/ (users table), kafka/AccountsListener, core/Conversations, core/Ingest      (Task 10)
platform-devenv/docker-compose.yml, Makefile                                                        (Task 2, 11)
gateway-devtools/.../cmd/AccountCommand.java, ChatSmoke.java, SmokeCommand.java                    (Task 11)
docs/adr/0033–0036, README, CHANGELOG, account-service/README.md                                   (Task 11)
```

---

### Task 1: Contracts, scope `accounts:admin`, seed `chat-admin`

**Files:**
- Create: `platform-contracts/proto/pangate/v1/account.proto`, `platform-contracts-testkit/src/main/java/com/pangate/contracts/testkit/AccountTopics.java`
- Modify: `auth-service/src/main/java/com/pangate/auth/domain/Scope.java`, `auth-service/src/main/java/com/pangate/auth/config/SeedRunner.java`
- Test: `platform-contracts/src/test/java/com/pangate/contracts/AccountContractNumbersTest.java`, `auth-service/src/test/.../domain/ScopeTest.java` (thêm case), `auth-service/src/it/.../SeedIT.java` (thêm case)

**Interfaces:**
- Produces: proto `AccountState { string user_id = 1; Status status = 2; int64 updated_at_ms = 3; }`, enum `AccountState.Status { STATUS_UNSPECIFIED = 0; STATUS_ACTIVE = 1; STATUS_LOCKED = 2; STATUS_DELETED = 3; }` (đúng spec §6). `AccountTopics.USERS = "account.users"`, `AccountTopics.PARTITIONS = 12`. `Scope.ACCOUNTS_ADMIN("accounts:admin")`. Seed client `chat-admin` (tenant `chat`, scope `accounts:admin`), secret ghi vào `clients.json` như các client khác.

- [ ] **Step 1:** `AccountContractNumbersTest.fieldNumbersArePinned`: `AccountState.USER_ID_FIELD_NUMBER == 1`, `STATUS_FIELD_NUMBER == 2`, `UPDATED_AT_MS_FIELD_NUMBER == 3`, `Status.STATUS_DELETED_VALUE == 3`. `ScopeTest.accountsAdminParses`: `Scope.parse("accounts:admin") == ACCOUNTS_ADMIN`. `SeedIT.chatAdminClientIsSeeded`: `clients.json` có `chat-admin` với scope `accounts:admin`; `/oauth2/token` với secret đó trả token có scope `accounts:admin`, tenant `chat`.
- [ ] **Step 2:** `./mvnw -f platform-contracts test -Dtest=AccountContractNumbersTest`; `./mvnw -f auth-service verify -Dtest=ScopeTest -Dit.test=SeedIT`. Expected: FAIL (thiếu class / scope / client).
- [ ] **Step 3:** Viết proto, `AccountTopics`, thêm scope và seed.
- [ ] **Step 4:** Chạy lại; `./mvnw -f platform-contracts install`, `-f platform-contracts-testkit install`, `./mvnw -f auth-service verify`. Expected: PASS.
- [ ] **Step 5:** Commit `feat(contracts,auth): AccountState, account.users topic, accounts:admin scope and chat-admin seed`.

---

### Task 2: Khung `account-service`, schema PostgreSQL, devenv

**Files:**
- Create: `account-service/pom.xml` (theo `auth-service/pom.xml`: web, actuator, jdbc, flyway-core + flyway-database-postgresql, postgresql, kafka-clients, lettuce-core, nimbus-jose-jwt, spring-security-crypto + bcprov, micrometer-prometheus, starter, contracts; test: starter-test, awaitility, testcontainers junit-jupiter / postgresql / kafka, `auth-service` (test scope, để IT chạy auth-service thật trong JVM như chat-service chạy push-api); plugin build-helper (`src/it/java`), surefire, failsafe, spring-boot (classifier `exec`), jib), `AccountApplication`, `config/AccountProperties`, `config/AdminConnector` (cổng 8081, như auth-service), `api/PortRoutingFilter` (`/admin/**` chỉ trên cổng quản trị, `/v1/**` chỉ trên 8080), `tools/HealthCheck` (chép từ chat-service), `resources/application.yml`, `application-local.yml`, `application-dev.yml`, `db/migration/V1__init.sql` (đúng spec §3, kèm `CREATE SCHEMA IF NOT EXISTS account` qua Flyway `schemas: account`)
- Modify: `platform-devenv/docker-compose.yml` (service `account`, profile `app`, image `pangate/account-service:dev`, cổng `8088:8080`, `8089:8081`, volume `dev-secrets` để đọc `clients.json`, depends_on `postgres`, `redis`, `kafka-init`, `auth` healthy; `kafka-init` tạo `account.users` 12 partition `--config cleanup.policy=compact`); `Makefile` (MODULES thêm `account-service` sau `chat-service`; `images` thêm `jib:dockerBuild` cho `account-service`)
- Test: `account-service/src/test/java/com/pangate/account/config/AccountPropertiesTest.java`, `account-service/src/it/java/com/pangate/account/SchemaIT.java`

**Interfaces:**
- Produces: `record AccountProperties(Duration sessionIdleTtl /*180d*/, Duration lastUsedWriteEvery /*5m*/, int maxConcurrentHashes /*0 = 2×cores*/, List<String> reservedUsernames, int trustedProxies /*1*/, Auth auth, Kafka kafka, Redis redis, Limits limits, Outbox outbox)` với `Auth(String baseUrl /*http://auth:8090*/, String issuer, String jwksUri, String clientId /*chat-backend*/, String clientSecretName /*account/chat-backend-secret*/, Path clientFile /*null; dev: /dev-secrets/auth/clients.json*/, String adminTenant /*chat*/)`, `Kafka(String bootstrap, String usersTopic /*account.users*/, String controlTopic /*gw.control*/)`, `Redis(String uri, Duration timeout /*200ms*/)`, `Limits(...)` giữ các con số spec §7, `Outbox(Duration interval /*200ms*/, int batch /*500*/, Duration keepSent /*7d*/)`. Mặc định `reservedUsernames = [admin, administrator, support, pangate, system, root, chat, official]` (spec §12 để người hiện thực chọn; đây là lựa chọn của plan).

- [ ] **Step 1:** `AccountPropertiesTest.defaultsMatchSpec` (giá trị mặc định trên). `SchemaIT.migrationCreatesTheSpecTables`: PostgreSQL container; khởi động app với Flyway → bảng `accounts`, `identifiers`, `sessions`, `admin_actions`, `outbox` có trong schema `account`; `accounts.username` kiểu `citext`; chèn hai dòng `identifiers` cùng `(email, x)` → vi phạm khoá chính.
- [ ] **Step 2:** `./mvnw -f account-service verify`. Expected: FAIL (module chưa có).
- [ ] **Step 3:** Hiện thực. `/ready` dùng health group gồm `db`. Profile `local`: datasource `localhost:5432/pangate`, Kafka `localhost:9092`, Redis `localhost:6379`, auth `http://localhost:8090`, client file `.dev/auth/clients.json`. Profile `dev`: các host trong compose.
- [ ] **Step 4:** `./mvnw -f account-service verify`; `docker compose -f platform-devenv/docker-compose.yml config -q`. Expected: PASS.
- [ ] **Step 5:** Commit `feat(account): account-service skeleton, PostgreSQL schema, devenv`.

---

### Task 3: Quy tắc dữ liệu, mật khẩu, refresh token

**Files:**
- Create: `domain/Username.java`, `domain/Email.java`, `domain/Phone.java`, `domain/Login.java`, `domain/LoginKind.java`, `domain/PasswordPolicy.java`, `domain/ProfileRules.java`, `domain/DomainException.java`; `application/port/PasswordHasher.java`; `infrastructure/crypto/Argon2PasswordHasher.java`, `infrastructure/crypto/HashGate.java`, `infrastructure/crypto/RefreshTokens.java`
- Test: `domain/LoginKeysTest`, `domain/PasswordPolicyTest`, `domain/ProfileRulesTest`, `infrastructure/crypto/HashGateTest`, `infrastructure/crypto/RefreshTokensTest`

**Interfaces:**
- Produces:
  - `Username.of(String raw, Set<String> reserved): Username` (giữ cách viết gốc `display()`, khoá so sánh `key()` = chữ thường); ném `DomainException("invalid_username")`.
  - `Email.of(String raw): Email` (`value()` đã cắt khoảng trắng, chữ thường); `Phone.of(String raw): Phone`; ném `invalid_email` / `invalid_phone`.
  - `Login.parse(String raw): Login` với `record Login(LoginKind kind, String key)`, `enum LoginKind { EMAIL, PHONE, USERNAME }`: chứa `@` → EMAIL; bắt đầu bằng `+` → PHONE; còn lại → USERNAME (`key` chữ thường). Không ném: login sai định dạng vẫn đi tiếp để trả `invalid_credentials` như mọi lỗi đăng nhập.
  - `PasswordPolicy.check(char[] password, Username u, Email e /*nullable*/, Phone p /*nullable*/): char[]` (trả bản NFC; ném `weak_password`).
  - `ProfileRules.displayName(String): String`, `ProfileRules.bio(String): String` (ném `invalid_display_name`, `invalid_bio`).
  - `interface PasswordHasher { String hash(char[] pw); boolean matches(char[] pw, String hash); void burn(); }` (`burn` băm giả để cân thời gian).
  - `HashGate(int permits)`: `<T> T run(Supplier<T> work)`; không lấy được permit ngay → ném `BusyException` (api trả `503` + `Retry-After: 1`).
  - `RefreshTokens.newToken(): String` (`rt_` + base64url 32 byte), `RefreshTokens.hash(String token): byte[]` (SHA-256), `RefreshTokens.looksValid(String): boolean`.

- [ ] **Step 1:** Test:
  - `LoginKeysTest.normalisationMakesOneKey` (Review Focus 1): `Email.of(" Lan@Mail.com ").value()` = `lan@mail.com`; `Username.of("LanNguyen", …).key()` = `lannguyen`, `display()` = `LanNguyen`; `Login.parse("LAN@mail.com")` = `(EMAIL, lan@mail.com)`; `Login.parse("+84901234567")` = `(PHONE, +84901234567)`; `Login.parse("LanNguyen")` = `(USERNAME, lannguyen)`.
  - `LoginKeysTest.usernameRules`: `"abcd"` (4) → invalid; `"abcde"` hợp lệ; 32 ký tự hợp lệ, 33 → invalid; `"1abcde"` → invalid; `"Admin"` → invalid (cấm); `"a_b_c"` hợp lệ.
  - `LoginKeysTest.phoneAndEmailRules`: `"0901234567"` → invalid_phone; `"+84901234567"` hợp lệ; email 255 ký tự → invalid; `"a@b"` → invalid; `"a@b.vn"` hợp lệ.
  - `PasswordPolicyTest.lengthIsCountedInCharacters`: 7 ký tự → weak; 8 hợp lệ; 128 hợp lệ; 129 → weak; `"🔐".repeat(8)` hợp lệ (8 code point).
  - `PasswordPolicyTest.nfcAndNfdAreTheSamePassword` (Review Focus 2): hash bản NFC của `"café123"`, `matches` với bản đã qua `check` của `"café123"` → true.
  - `PasswordPolicyTest.mustNotEqualIdentifiers`: mật khẩu = username (khác hoa thường) → weak; = email → weak.
  - `ProfileRulesTest`: displayName `"  "` → invalid; 64 ký tự hợp lệ, 65 → invalid; có `\u0007` → invalid; bio 141 → invalid.
  - `HashGateTest.busyWhenAllPermitsTaken`: `HashGate(1)`, một tác vụ đang giữ permit → tác vụ thứ hai ném `BusyException` ngay.
  - `RefreshTokensTest`: token bắt đầu `rt_`, dài 46 ký tự, hai token khác nhau; `hash` dài 32 byte và ổn định.
- [ ] **Step 2:** `./mvnw -f account-service test`. Expected: FAIL (chưa có class).
- [ ] **Step 3:** Hiện thực; `Argon2PasswordHasher` như `auth-service` `Argon2SecretHasher` (OWASP m=19456 KiB, t=2, p=1), nhận `char[]` đã NFC.
- [ ] **Step 4:** `./mvnw -f account-service test`. Expected: PASS.
- [ ] **Step 5:** Commit `feat(account): data rules, Argon2id passwords with a hashing gate, refresh tokens`.

---

### Task 4: Store PostgreSQL và outbox

**Files:**
- Create: `domain/Account.java`, `domain/AccountStatus.java`, `domain/Session.java`; `application/port/{AccountStore,SessionStore,Outbox,AdminLog,Transactions,Clock}.java`; `infrastructure/postgres/{JdbcAccountStore,JdbcSessionStore,JdbcOutbox,JdbcAdminLog,AdvisoryLock}.java`
- Test: `account-service/src/it/java/com/pangate/account/StoreIT.java`

**Interfaces:**
- Produces:
  - `record Account(String userId, String username /*display*/, String displayName, String bio, String passwordHash, boolean mustChangePassword, AccountStatus status, String email, String phone, Instant createdAt, Instant deletedAt)`; `enum AccountStatus { ACTIVE, LOCKED, DELETED }`.
  - `record Session(String deviceId, String userId, String deviceName, String platform, Instant createdAt, Instant lastUsedAt, Instant revokedAt)`.
  - `AccountStore`: `void insert(Account a)` (ném `TakenException(String code)` với `username_taken` / `email_taken` / `phone_taken` theo ràng buộc bị vi phạm); `Optional<Account> byId(String userId)`; `Optional<Account> byLogin(Login l)`; `Optional<Account> byUsernameKey(String key)`; `void updateProfile(String userId, String displayName, String bio)`; `void updateUsername(String userId, Username u)`; `void setIdentifier(String userId, LoginKind kind, String valueOrNull)`; `void setPassword(String userId, String hash, boolean mustChange)`; `void setStatus(String userId, AccountStatus s)`; `void erase(String userId)` (spec §5.3: NULL dữ liệu cá nhân, xoá `identifiers`, `status=deleted`, `deleted_at`); `List<Account> byIds(Collection<String> ids)`.
  - `SessionStore`: `void insert(Session s, byte[] refreshHash)`; `Optional<Session> byRefreshHash(byte[] h)`; `List<Session> live(String userId)`; `List<String> revokeAll(String userId, String exceptDeviceId /*nullable*/)` (trả các `deviceId` vừa thu hồi); `boolean revoke(String userId, String deviceId)`; `void touch(String deviceId, Instant at)`; `int expireIdle(Instant before)`; `int purgeRevoked(Instant before)`; `boolean isLive(String deviceId)`.
  - `Outbox`: `void event(String userId, AccountState state)`; `void revokeDevice(String userId, String deviceId)`; `void revokePrincipal(String userId)`; `List<OutboxRow> pending(String kind, int limit)`; `void markSent(long id)`; `int purgeSent(Instant before)`; `record OutboxRow(long id, String kind, String key, byte[] payload)`. Payload thu hồi là body JSON đúng `POST /v1/revocations` của auth-service (`{"kind":"device","principal":"chat:<u>","device":"<d>"}`, `{"kind":"principal","principal":"chat:<u>"}`).
  - `AdminLog.record(String actor, String action, String userId, String reason)`; `List<AdminActionRow> of(String userId)`.
  - `Transactions.inTx(Supplier<T>)` (Spring `TransactionTemplate`).
- Mọi lỗi kết nối DB → `StoreUnavailableException` (api: `503` + `Retry-After: 5`).

- [ ] **Step 1:** `StoreIT`:
  - `concurrentRegistrationsOfOneUsernameHaveOneWinner`: 8 luồng `insert` cùng username khác email → đúng 1 thành công, 7 `TakenException("username_taken")`.
  - `takenCodeNamesTheField`: email trùng → `email_taken`; phone trùng → `phone_taken`.
  - `byLoginFindsByEveryKind`: tìm được bằng email, phone, username khác hoa thường.
  - `eraseFreesIdentifiersAndKeepsTheRow`: sau `erase`, `byId` còn với status DELETED và mọi trường cá nhân null; `insert` tài khoản mới cùng username / email thành công.
  - `revokeAllExceptKeepsTheCurrentDevice`: 3 phiên, `revokeAll(u, d2)` trả `[d1, d3]`, `live(u)` = `[d2]`.
  - `outboxKeepsOrderPerKind`: ghi event, revoke, event → `pending("event")` theo `id` tăng dần, không lẫn dòng revocation.
- [ ] **Step 2:** `./mvnw -f account-service verify -Dit.test=StoreIT`. Expected: FAIL.
- [ ] **Step 3:** Hiện thực bằng `JdbcClient`; phân biệt ràng buộc bằng tên constraint của PostgreSQL (`accounts_username_key`, `identifiers_pkey` + `kind`).
- [ ] **Step 4:** Chạy lại. Expected: PASS 6/6.
- [ ] **Step 5:** Commit `feat(account): PostgreSQL stores and outbox`.

---

### Task 5: Đăng ký, đăng nhập, rate limit

**Files:**
- Create: `application/Register.java`, `application/Login.java` (use case, tên lớp `LoginUseCase` để tránh trùng `domain.Login`), `application/port/AttemptCounter.java`, `infrastructure/redis/RedisAttemptCounter.java` (fallback `InMemoryAttemptCounter` khi Redis lỗi), `api/Problems.java`, `api/ProblemHandler.java`, `api/ClientIp.java`, `api/AccountsController.java` (`POST /v1/accounts`), `api/SessionsController.java` (`POST /v1/sessions`)
- Test: `application/RegisterTest`, `application/LoginUseCaseTest`, it `RegisterLoginIT`

**Interfaces:**
- Consumes: Task 3, Task 4.
- Produces:
  - `record NewSession(String userId, String deviceId, String refreshToken)`; `record DeviceInfo(String deviceName, String platform)`.
  - `Register.register(String username, char[] password, String email, String phone, String displayName, DeviceInfo d, String ip): NewSession`: một transaction: `accounts`, `identifiers`, phiên đầu, outbox `AccountState(ACTIVE)`.
  - `LoginUseCase.login(String login, char[] password, char[] newPassword /*nullable*/, DeviceInfo d, String ip): NewSession`; lỗi: `invalid_credentials` (401), `account_locked` (403), `password_change_required` (403), `too_many_attempts` (429, kèm `retryAfterSec`), `busy` (503).
  - `AttemptCounter`: `long failures(String key, Duration window)`, `void fail(String key, Duration window)`, `void clear(String key)`, `boolean blocked(String key)`, `void block(String key, Duration d)`, `long incrementWithin(String key, Duration window)` (đếm đăng ký). Khoá: `login:<loginKey>|<ip>`, `ip:<ip>`, `loginall:<loginKey>`, `reg:<ip>`.
  - `ClientIp.of(HttpServletRequest, int trustedProxies): String` (`X-Forwarded-For`, phần tử thứ `trustedProxies` tính từ phải; không có thì `remoteAddr`).

- [ ] **Step 1:** Test (unit dùng store và counter giả; IT dùng PostgreSQL + Redis):
  - `LoginUseCaseTest.wrongLoginAndWrongPasswordLookTheSame`: login không tồn tại và mật khẩu sai đều ném `invalid_credentials`; login không tồn tại gọi `PasswordHasher.burn()` đúng 1 lần.
  - `LoginUseCaseTest.tenFailuresBlockThatLoginFromThatIpOnly`: 10 lần sai từ IP A → lần 11 (kể cả mật khẩu đúng) `too_many_attempts`; cùng login từ IP B với mật khẩu đúng → thành công.
  - `LoginUseCaseTest.fiftyFailuresBlockTheIp`, `LoginUseCaseTest.hundredFailuresFromManyIpsSlowTheLoginDown` (lần thứ 101 chờ ≥ 2 s qua `Clock`/sleeper giả, không ném).
  - `LoginUseCaseTest.lockedAccountIs403AndDeletedLooksUnknown`.
  - `LoginUseCaseTest.mustChangePassword`: không có `newPassword` → `password_change_required`; có `newPassword` hợp lệ → đổi hash, `mustChangePassword=false`, trả phiên.
  - `RegisterTest.needsEmailOrPhone` (`invalid_request`), `RegisterTest.sixthRegistrationFromOneIpInAnHourIs429`.
  - `RegisterLoginIT.registerThenLoginWithEachKind`: `POST /v1/accounts` → `201 {userId, deviceId, refreshToken}`; outbox có `AccountState ACTIVE`; đăng nhập bằng email, phone, username đều `201`, `deviceId` mới mỗi lần.
  - `RegisterLoginIT.loginIsCaseAndSpaceInsensitive` (Review Focus 1).
  - `RegisterLoginIT.takenFieldsAre409`: `username_taken`, `email_taken`, `phone_taken` → `409` problem+json.
  - `RegisterLoginIT.redisDownStillLimits`: Redis dừng → đăng nhập vẫn chạy, giới hạn theo bộ nhớ của pod vẫn chặn lần sai thứ 11.
- [ ] **Step 2:** `./mvnw -f account-service verify -Dtest='RegisterTest,LoginUseCaseTest' -Dit.test=RegisterLoginIT`. Expected: FAIL.
- [ ] **Step 3:** Hiện thực. Mã HTTP: `invalid_*`/`weak_password` 400, `*_taken` 409. Metric `account.registrations{result}`, `account.logins{result}`, `account.ratelimited{what}`.
- [ ] **Step 4:** Chạy lại. Expected: PASS.
- [ ] **Step 5:** Commit `feat(account): registration and password login with attempt limits`.

---

### Task 6: Client auth-service, hai relay, job hết hạn phiên

**Files:**
- Create: `application/port/AuthApi.java`, `infrastructure/authclient/HttpAuthApi.java`, `config/AuthClientSecret.java`, `infrastructure/kafka/EventRelay.java`, `infrastructure/kafka/AccountStateMapper.java`, `application/RevocationRelay.java`, `application/ExpireSessions.java`, `config/JobsConfig.java`
- Test: `application/RevocationRelayTest`, it `RelaysIT`

**Interfaces:**
- Produces:
  - `interface AuthApi { ConnectToken mint(String userId, String deviceId); void revoke(byte[] revocationJson); }` với `record ConnectToken(String token, Instant expiresAt)`; ném `AuthUnavailableException` (5xx, timeout, 429) hoặc `AuthRejectedException(int status)` (4xx khác).
  - `HttpAuthApi`: `POST {baseUrl}/oauth2/token` (client credentials `chat-backend`, scope `tokens:mint sessions:revoke`), cache token API tới 60 s trước `exp`; `POST /v1/tokens`; `POST /v1/revocations`; timeout 2 s.
  - `AuthClientSecret.load(AccountProperties.Auth, SecretSource): String`: có `clientFile` thì đọc secret của `clientId` trong `clients.json` (định dạng do `SeedRunner` của auth-service ghi), không thì `SecretSource.get(clientSecretName)`.
  - `EventRelay.runOnce(): int` và `RevocationRelay.runOnce(): int` (số dòng đã gửi); mỗi job một advisory lock; lịch 200 ms trong `JobsConfig`; `ExpireSessions.runOnce()` mỗi giờ.

- [ ] **Step 1:** Test:
  - `RevocationRelayTest.transientErrorStopsTheRoundInOrder`: 3 dòng, `AuthApi` lỗi `AuthUnavailableException` ở dòng 2 → chỉ dòng 1 `sent`; lượt sau gửi 2 rồi 3.
  - `RevocationRelayTest.permanentErrorIsSkipped`: dòng 2 ném `AuthRejectedException(403)` → đánh dấu đã gửi, dòng 3 vẫn gửi; counter `account.revocations.rejected` = 1.
  - `RelaysIT.eventsReachTheCompactedTopicInOrder`: PostgreSQL + Kafka; outbox ACTIVE rồi DELETED cho cùng user → đọc `account.users` thấy hai bản theo thứ tự, key = userId.
  - `RelaysIT.kafkaDownDoesNotDelayRevocations`: Kafka dừng; ghi một revoke → auth-service thật (khởi động trong JVM bằng `AuthStack`, xem dưới) nhận `revoke:d:chat:<u>/<d>` trên `gw.control` trong < 5 s.
  - `RelaysIT.mintedTokenHasTheSessionDevice`: `HttpAuthApi.mint(u, d)` → JWT có `sub=chat:<u>`, `device=<d>`, `aud=pangate-gw`, `exp - iat = 600`.
- [ ] **Step 2:** `./mvnw -f account-service verify -Dtest=RevocationRelayTest -Dit.test=RelaysIT`. Expected: FAIL.
- [ ] **Step 3:** Hiện thực. Metric `account.outbox.pending`, `account.outbox.oldest_age_seconds` (gauge theo `kind`), `account.revocations.rejected`, `account.sessions.active` (gauge, `count(*)` phiên sống, đọc lại mỗi 60 s). `/ready` không phụ thuộc Kafka hay auth-service (outbox dồn lại rồi gửi bù, như auth-service). Thêm `src/it/java/com/pangate/account/support/AuthStack.java`: khởi động `AuthApplication` trong JVM (PostgreSQL và Kafka của test, schema `auth`, seed bật, seed dir tạm) và trả `baseUrl` + đường dẫn `clients.json`; dùng chung cho Task 7–9.
- [ ] **Step 4:** Chạy lại. Expected: PASS.
- [ ] **Step 5:** Commit `feat(account): auth-service client, event and revocation relays, session expiry`.

---

### Task 7: Đổi token và quản lý phiên

**Files:**
- Create: `application/ExchangeToken.java`, `application/ManageSessions.java`; `infrastructure/auth/{ConnectTokenVerifier,Revocations,RevocationListener,JwksSource,HttpJwksSource}.java` (chép từ `chat-service/.../auth/` rồi đổi package; §12 của spec để ngỏ, plan chọn chép để CH2 không phải sửa chat-service); `api/ConnectAuthFilter.java`; mở rộng `api/SessionsController.java`
- Test: `application/ExchangeTokenTest`, it `SessionsIT`

**Interfaces:**
- Consumes: Task 4 `SessionStore`, Task 6 `AuthApi`, `Outbox`.
- Produces:
  - `ExchangeToken.exchange(String refreshToken): ConnectToken`; lỗi `invalid_session` (401), `account_locked` (403), `rate_limited` (429, 60/giờ/phiên), `unavailable` (503 khi `AuthUnavailableException`). Ghi `touch` khi `lastUsedAt` cũ hơn `lastUsedWriteEvery`.
  - `ConnectAuthFilter` trên `/v1/**` trừ `POST /v1/accounts`, `POST /v1/sessions`, `POST /v1/sessions/token`: kiểm connect token (như chat-service) rồi `SessionStore.isLive(deviceId)` qua cache 10 s; đặt `record Caller(String userId, String deviceId)` vào request.
  - `ManageSessions.list(Caller): List<SessionView>`, `logout(Caller, String deviceId)`, `logoutAll(Caller, boolean exceptCurrent)`: một transaction thu hồi phiên + outbox `revokeDevice` cho từng phiên.
  - HTTP: `POST /v1/sessions/token`, `GET /v1/sessions`, `DELETE /v1/sessions/{deviceId}`, `DELETE /v1/sessions?others=true` (spec §4.2).

- [ ] **Step 1:** Test:
  - `ExchangeTokenTest.revokedOrIdleSessionIs401`, `.lockedIs403`, `.lastUsedIsWrittenAtMostEveryFiveMinutes` (hai lần đổi cách 1 phút → 1 lần `touch`).
  - `SessionsIT` (PostgreSQL, Redis, Kafka, `AuthStack`; account-service khởi động trong JVM):
    - `exchangeGivesAConnectTokenForThisDevice`.
    - `logoutOneDeviceRevokesOnlyIt`: hai thiết bị; `DELETE /v1/sessions/{d1}` → `gw.control` có `revoke:d:chat:<u>/<d1>`, không có `revoke:p:`; refresh token của d1 → 401; d2 vẫn đổi được token.
    - `logoutOthersKeepsTheCaller`.
    - `passwordChangeKeepsTheCurrentDeviceConnected` (Review Focus 4; chạy được sau Task 8, đánh dấu `@Disabled` tới đó rồi bỏ ở Task 8): đổi mật khẩu từ d2 → chỉ d1 bị thu hồi; d2 đổi token hai lần song song đều thành công.
    - `connectTokenOfLoggedOutDeviceIsRejectedByAccountApi`: sau khi đăng xuất d1, `GET /v1/sessions` bằng connect token cũ của d1 → 401 trong ≤ 10 s.
- [ ] **Step 2:** `./mvnw -f account-service verify -Dtest=ExchangeTokenTest -Dit.test=SessionsIT`. Expected: FAIL.
- [ ] **Step 3:** Hiện thực; metric `account.tokens{result}`.
- [ ] **Step 4:** Chạy lại. Expected: PASS (case `@Disabled` bỏ qua).
- [ ] **Step 5:** Commit `feat(account): refresh token exchange and device sessions`.

---

### Task 8: Hồ sơ, tìm user, đổi mật khẩu và định danh

**Files:**
- Create: `application/Profiles.java`, `application/ChangePassword.java`, `application/ChangeIdentifier.java`; `api/MeController.java`, `api/UsersController.java`
- Test: `application/ProfilesTest`, `application/ChangeIdentifierTest`, it `ProfileIT`

**Interfaces:**
- Produces:
  - `record PublicProfile(String userId, String username, String displayName, String bio, boolean deleted)` (JSON: khi `deleted` chỉ có `userId` và `deleted: true`).
  - `Profiles.me(Caller): MeView`, `update(Caller, String displayName, String bio)`, `changeUsername(Caller, String)`, `byId(String): PublicProfile`, `byUsername(String): Optional<PublicProfile>`, `batch(List<String> ids): List<PublicProfile>` (> 100 id → `invalid_request`).
  - `ChangePassword.change(Caller, char[] current, char[] next)`: sai mật khẩu hiện tại → `invalid_credentials` (401, tính vào giới hạn đăng nhập sai của login + IP); thành công → `revokeAll(userId, caller.deviceId)` + outbox `revokeDevice` từng phiên.
  - `ChangeIdentifier.set(Caller, LoginKind kind, String valueOrNull, char[] password)`: gỡ cả hai → `invalid_request`.
  - HTTP đúng bảng spec §4.2 cho `/v1/me*` và `/v1/users*`; giới hạn tra user 600/phút/user.

- [ ] **Step 1:** Test:
  - `ProfilesTest.batchSkipsUnknownIdsAndMarksDeleted`, `.batchOver100Is400`, `.lockedUserStillHasAProfile`.
  - `ChangeIdentifierTest.cannotRemoveTheLastIdentifier`, `.wrongPasswordIs401`.
  - `ProfileIT.renameFreesTheOldUsername`: A đổi `alice1` → `alice2`; B đăng ký `alice1` thành công; `GET /v1/users?username=ALICE2` trả A.
  - `ProfileIT.passwordChangeLogsOutOtherDevices`; bỏ `@Disabled` ở `SessionsIT.passwordChangeKeepsTheCurrentDeviceConnected` và chạy nó.
- [ ] **Step 2:** `./mvnw -f account-service verify -Dtest='ProfilesTest,ChangeIdentifierTest' -Dit.test='ProfileIT,SessionsIT'`. Expected: FAIL.
- [ ] **Step 3:** Hiện thực.
- [ ] **Step 4:** Chạy lại. Expected: PASS.
- [ ] **Step 5:** Commit `feat(account): profiles, username lookup, password and identifier changes`.

---

### Task 9: Admin API và xoá tài khoản

**Files:**
- Create: `application/AdminAccounts.java`, `application/DeleteAccount.java`; `infrastructure/auth/ApiTokenVerifier.java` (kiểm token API: `aud=pangate-api`, `iss`, `exp`, scope `accounts:admin`, `tenant` = `adminTenant`, client chưa bị thu hồi qua khoá `revoke:c:` trên `gw.control`; theo `push-api-service` `ApiTokenVerifier`); `api/AdminAuthFilter.java`, `api/AdminController.java`; thêm `DELETE /v1/me` vào `MeController`
- Test: `application/AdminAccountsTest`, `application/DeleteAccountTest`, it `AdminAndDeleteIT`

**Interfaces:**
- Produces:
  - `AdminAccounts.get(String userId)`, `find(LoginKind kind, String value)`, `lock(String actor, String userId, String reason)`, `unlock(...)`, `resetPassword(...): String` (mật khẩu tạm 16 ký tự từ bảng chữ + số, `SecureRandom`), `delete(...)`; thiếu hoặc rỗng `reason` → `invalid_request`; mỗi thao tác ghi `AdminLog` và metric `account.admin.actions{action}`.
  - `DeleteAccount.delete(String userId)`: một transaction: `revokeAll`, outbox `revokeDevice` từng phiên **và** `revokePrincipal`, `erase`, outbox `AccountState(DELETED)`. Dùng cho `DELETE /v1/me` (cần `{password}`) và admin.
  - `lock`: `setStatus(LOCKED)`, `revokeAll` + `revokeDevice` từng phiên, outbox `AccountState(LOCKED)`; `unlock`: `ACTIVE` + `AccountState(ACTIVE)`.
  - HTTP đúng bảng spec §5; body `{reason}`; giới hạn 20/s/client.

- [ ] **Step 1:** Test:
  - `AdminAccountsTest.reasonIsRequired`, `.resetForcesPasswordChangeAndRevokesEverySession`, `.lockRevokesDevicesNotThePrincipal` (outbox chỉ có `device`).
  - `DeleteAccountTest.deleteRevokesThePrincipalAndPublishesDeleted`.
  - `AdminAndDeleteIT` (`AuthStack`, token API của `chat-admin`):
    - `adminNeedsTheScope`: token `chat-backend` → 403; không token → 401; gọi admin trên cổng 8080 → 404.
    - `lockThenUnlock`: khoá → đăng nhập `403 account_locked`, `gw.control` có `revoke:d:` cho từng thiết bị và không có `revoke:p:`; mở khoá → đăng nhập và đổi token được ngay.
    - `deletedIdentifiersAreReusableAndOldIdStaysDeleted` (Review Focus 3).
    - `selfDeleteNeedsThePassword`.
- [ ] **Step 2:** `./mvnw -f account-service verify -Dtest='AdminAccountsTest,DeleteAccountTest' -Dit.test=AdminAndDeleteIT`. Expected: FAIL.
- [ ] **Step 3:** Hiện thực.
- [ ] **Step 4:** `./mvnw -f account-service verify` (toàn bộ). Expected: PASS.
- [ ] **Step 5:** Commit `feat(account): admin lock, unlock, password reset and account deletion`.

---

### Task 10: chat-service biết user (`account.users`)

**Files:**
- Create: `chat-service/src/main/resources/cql/V2__users.cql` (`users(user_id text PRIMARY KEY, status text, updated_at bigint)`), `chat-service/src/main/java/com/pangate/chat/kafka/AccountsListener.java`, `chat-service/src/main/java/com/pangate/chat/core/UserDirectory.java`, `chat-service/src/main/java/com/pangate/chat/core/AccountRemoval.java`
- Modify: `store/ScyllaChatStore.java` (bảng `users`; `MemberStore.joinedAt` để chọn owner mới), `core/ChatStores.java` (interface `UserStore`), `core/Conversations.java` (kiểm user khi tạo DM / nhóm / kênh và thêm thành viên), `core/Ingest.java` (DM với người đã xoá → `Rejection("user_deleted")`), `http/SendController.java` / `http/ProblemHandler.java` (`user_deleted`, `user_not_found` → 404), `config/ChatConfig.java` (listener, consumer group `chat-accounts`), `src/test/.../core/FakeStores.java`, `src/it/.../support/ChatStack.java` và `ChatHarness.java` (helper `knownUser(String userId)` ghi `AccountState ACTIVE` lên `account.users` và chờ chat-service thấy; mọi IT hiện có gọi nó cho user của mình), `gateway-devtools` không đổi ở task này
- Test: `core/ConversationsTest` (thêm case), `core/IngestTest` (thêm case), `core/AccountRemovalTest`, it `kafka/AccountsListenerIT`

**Interfaces:**
- Produces:
  - `interface UserStore { Optional<UserRecord> user(String userId); boolean upsertIfNewer(String userId, String status, long updatedAtMs); }`; `record UserRecord(String userId, String status, long updatedAtMs)`; `status` ∈ `active`, `deleted` (`LOCKED` lưu là `active`).
  - `UserDirectory.requireActive(String userId)` → ném `ChatRefusal.notFound("user_not_found")` / `ChatRefusal("user_deleted")`.
  - `AccountRemoval.removed(String userId)`: theo `user_convs` của user: nhóm / kênh → rời hội thoại (đường `Conversations.removeMember` của CH1, kể cả thu hồi subscription kênh), owner → chuyển cho admin `joined_at` sớm nhất, không có thì member sớm nhất; DM giữ nguyên. Idempotent.
  - `AccountsListener`: đọc `account.users` từ đầu (group `chat-accounts`, `auto.offset.reset=earliest`), `upsertIfNewer`; lần đầu chuyển sang `deleted` thì gọi `AccountRemoval.removed`; lỗi tạm thì tua lại như các loop CH1.

- [ ] **Step 1:** Test:
  - `ConversationsTest.unknownOrDeletedUserCannotBeAdded`: tạo DM / nhóm / thêm thành viên với user không có → `user_not_found`; đã xoá → `user_deleted`.
  - `IngestTest.dmWithDeletedUserIsRejected`.
  - `AccountRemovalTest.ownerPassesToTheEarliestAdminThenMember`, `.dmIsKept`, `.runningTwiceChangesNothing`.
  - `AccountsListenerIT.newUserIsKnownWithinTwoSeconds` (Review Focus 5): ghi `ACTIVE` → `user_not_found` biến mất trong ≤ 2 s.
  - `AccountsListenerIT.olderStateDoesNotOverwrite`: `DELETED@t2` rồi `ACTIVE@t1` → vẫn `deleted`.
  - `AccountsListenerIT.replayFromTheStartRebuildsUsers`: xoá bảng `users`, đổi group id → đọc lại đủ.
- [ ] **Step 2:** `./mvnw -f chat-service verify -Dtest='ConversationsTest,IngestTest,AccountRemovalTest' -Dit.test=AccountsListenerIT`. Expected: FAIL.
- [ ] **Step 3:** Hiện thực; cập nhật `ChatHarness`/`ChatStack` và các IT dùng user (`ChatE2EIT`, `QueryApiIT`, `SendHttpIT`, `SendAcceptIT`, `FanOutIT`, `SequencerIT`, `UpstreamCommandListenerIT`, `ChatLoadIT`) gọi `knownUser`.
- [ ] **Step 4:** `./mvnw -f chat-service verify` (toàn bộ). Expected: PASS, không IT nào hỏng.
- [ ] **Step 5:** Commit `feat(chat): users from account.users, deleted accounts leave conversations`.

---

### Task 11: Devtools, smoke, tài liệu

**Files:**
- Create: `gateway-devtools/src/main/java/com/pangate/devtools/cmd/AccountCommand.java` (`account register|login|token`, in JSON), `gateway-devtools/src/main/java/com/pangate/devtools/cmd/AccountSmoke.java`, `account-service/README.md`, `docs/adr/0033-…` tới `0036-…`
- Modify: `gateway-devtools/.../cmd/SmokeCommand.java` (`--account <url>`, `--admin-client-file` dùng lại `--client-file`), `gateway-devtools/.../cmd/ChatSmoke.java` (khi có `--account`: user là tài khoản đăng ký thật, token qua `/v1/sessions/token` thay vì `chat-backend`), `Makefile` (`smoke` thêm `--account http://account:8080`), `docs/adr/README.md`, `CHANGELOG.md`, `README.md` (bảng module), spec `2026-11-16-account-service-design.md` (dòng trạng thái)
- Test: `gateway-devtools/src/test/.../cmd/SmokeCommandTest.java` (thêm case), devenv `make images up smoke down`

**Interfaces:**
- Consumes: HTTP của Task 5–9.
- Produces: `AccountSmoke.run(Consumer<String> step)`: bước `ACCOUNT register`, `ACCOUNT login`, `ACCOUNT device logout` (4003 < 10 s, thiết bị kia vẫn mở), `ACCOUNT lock` (4003, đăng nhập 403), `ACCOUNT unlock` (HELLO được ngay); `CHAT 1-1`, `CHAT group`, `CHAT channel` của CH1 chạy bằng tài khoản thật.

- [ ] **Step 1:** `SmokeCommandTest.accountFlagAddsAccountSteps`: với `--account`, danh sách bước có 5 bước `ACCOUNT …` trước các bước `CHAT`; không có cờ thì như cũ.
- [ ] **Step 2:** `./mvnw -f gateway-devtools test -Dtest=SmokeCommandTest`. Expected: FAIL.
- [ ] **Step 3:** Hiện thực; viết ADR 0033–0036 (nội dung theo spec §11); README của module (cổng, cấu hình, API, giới hạn); CHANGELOG mục `account-service`; cập nhật trạng thái spec sau khi smoke xanh.
- [ ] **Step 4:** `./mvnw -f gateway-devtools test`; `make images up smoke down`. Expected: PASS; smoke in đủ bước `ACCOUNT …` và `CHAT …`.
- [ ] **Step 5:** Commit `feat(devtools): account commands and smoke; docs: ADR 0033-0036, account-service README`.
