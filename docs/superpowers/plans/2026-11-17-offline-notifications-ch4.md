# Thông báo đẩy khi offline CH4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Người không có thiết bị nào đang mở app vẫn nhận thông báo APNs/FCM có tên người gửi, trích đoạn và badge, cho DM, nhóm và kênh; push token gắn với thiết bị và bị gỡ khi thiết bị bị thu hồi.

**Architecture:** Service nền tảng mới `notify-service` (Spring Boot, PostgreSQL schema `notify`, Redis, Kafka), dựng theo khuôn của `account-service`: giữ token, nhận `NotifyRequest` trên `c1.notify.requests`, gửi APNs/FCM qua HTTP/2 của JDK, xoá token theo `gw.control`. chat-service thêm `OfflineNotifier` (đọc `c1.push.offline`) và `ChannelNotifier` (đọc `chat.channel.notify`), soạn thông báo trong `core/` (không import Spring) và produce `NotifyRequest`. account-service phát thêm tên hiển thị trên `account.users`.

**Tech Stack:** Java 21, Spring Boot 3.5.16, PostgreSQL 16 + Flyway, Lettuce, Kafka clients, `java.net.http.HttpClient` (HTTP/2), Nimbus JOSE (JWT ES256 / RS256, đã có trong account/chat); ScyllaDB cho chat; Testcontainers (Postgres, Redis, Kafka, Scylla).

**Spec:** `docs/superpowers/specs/2026-11-17-offline-notifications-design.md`

## Global Constraints

- Không sửa gateway, router, auth, Push API (spec §1).
- Contracts: file mới `pangate/v1/notify.proto` với `NotifyRequest`/`Alert` đúng field number spec §2.1; `AccountState.display_name = 4`; `ChannelNotifyJob { ChatMessage message = 1; }` trong `chat.proto`. `buf breaking` xanh.
- Topic: `c1.notify.requests` 32 partition, `retention.ms=86400000`, key = principal của người nhận; `chat.channel.notify` 16 partition, key = `conversationId` (spec §2.1, §4.1).
- Consumer group: `notify` (yêu cầu), `notify-revocations` (xoá token theo `gw.control`), `chat-notify` (`c1.push.offline`), `chat-channel-notify` (spec §3.1, §4.2, §5.3, §5.4).
- `request_id = "<msgId>/<userId>"`; dedup Redis `SET notifydedup:{request_id} NX EX 3600`, Redis lỗi thì vẫn gửi (spec §5.4).
- Tin cũ hơn 1 giờ không đẩy; `expires_at_ms = sentAt + 3 600 000`; `high_priority = true`; `thread_id = conversationId`; `collapse_id = msgId` (DM/nhóm) hoặc `conversationId` (kênh); `data = {conversationId, convSeq, msgId}` (spec §3.1, §3.5, §4.2).
- Nội dung cắt ở 200 code point + "…"; ẩn xem trước hoặc tin không có chữ → title/body trống, `loc_key = "notif_new_message"` (spec §3.5).
- Badge DM/nhóm = tổng `max(0, last_seq − read_seq)` trên các hội thoại chưa rời, gồm cả hội thoại đã tắt; kênh `badge = -1` (spec §3.1, §4.2, §9.2).
- Tắt thông báo: `muted_until` (0 = không, `Long.MAX_VALUE` = vĩnh viễn) trên `members` và `user_convs`; đang tắt vẫn đẩy nếu người nhận có trong `mentions` (spec §3.3).
- Token: APNs hex 64–200 ký tự; FCM ≤ 4 096 ký tự; đăng ký 10 lần/phút/thiết bị; dọn token không làm mới 180 ngày (spec §5.1).
- Kết quả gửi đúng bảng spec §5.4; thử lại tối đa 3 lần, chờ `Retry-After` hoặc 1 s / 4 s, không quá `expires_at_ms`; `max-in-flight` mặc định 1 000; JWT APNs làm mới mỗi 50 phút.
- Kênh: trang 1 000 người theo dõi, 5 000 `NotifyRequest`/s mỗi pod (`chat.notify.channel-rate`), một job sống mỗi kênh, điểm dừng `channel_notify_progress` TTL 1 ngày (spec §4.2).
- Lỗi HTTP của notify-service: `application/problem+json`, code `invalid_request`, `invalid_token`, `rate_limited`, `unavailable` như account-service.
- Metric đúng tên spec §6.
- Trước mỗi commit chạy `node .gitnexus/run.cjs detect-changes --scope all --repo .`; mỗi commit kết thúc bằng hai dòng trailer của session (Co-Authored-By, Claude-Session).

## Review Focus

1. **Cùng một điện thoại đổi tài khoản** (đăng xuất A, đăng nhập B, app đăng ký lại cùng token): A không bao giờ nhận thông báo trên máy đó nữa, kể cả khi lệnh thu hồi của A tới muộn. Test: `PushTokenStoreIT.sameTokenMovesToTheNewDevice` (Task 2).
2. **Đăng ký bằng connect token của thiết bị vừa bị thu hồi**: bị từ chối, không để lại token. Test: `RegistrationApiIT.revokedDeviceCannotRegister` (Task 2).
3. **Router ghi `OfflineRecord` hai lần cho một tin** (retry): đúng một thông báo trên máy. Test: `NotifyDispatcherTest.duplicateRequestIsSentOnce` (Task 5), `OfflineNotifierIT.replayedOfflineRecordNotifiesOnce` (Task 8).
4. **Token chết được báo về sau khi app đã đăng ký token mới**: token mới không bị xoá. Test: `NotifyDispatcherTest.deadTokenOnlyRemovedIfStillCurrent` (Task 5).
5. **Pod chết giữa job kênh**: pod khác đi tiếp từ điểm dừng, không ai nhận hai thông báo cho một bài. Test: `ChannelNotifierIT.resumesFromProgressAfterRestart` (Task 9).

---

### Task 1: Contracts, testkit, topic

**Files:**
- Create: `platform-contracts/proto/pangate/v1/notify.proto`
- Modify: `platform-contracts/proto/pangate/v1/account.proto` (`display_name = 4`), `platform-contracts/proto/pangate/v1/chat.proto` (`ChannelNotifyJob`)
- Create: `platform-contracts-testkit/src/main/java/com/pangate/contracts/testkit/{NotifyTopics,NotifyKeys,NotifyRules,NotifyCases}.java`
- Modify: `platform-contracts-testkit/.../ChatTopics.java` (`CHANNEL_NOTIFY = "chat.channel.notify"`)
- Test: `platform-contracts-testkit/src/test/java/com/pangate/contracts/testkit/NotifyRulesTest.java`
- Modify: `platform-devenv/docker-compose.yml` (kafka-init: `c1.notify.requests` 32 / 1 ngày, `chat.channel.notify` 16)

**Interfaces:**
- Produces: `com.pangate.contracts.v1.{NotifyRequest, Alert, ChannelNotifyJob}`; `NotifyTopics.requests(String cell)` → `"<cell>.notify.requests"`, `NotifyTopics.partitions(name)` = 32; `NotifyKeys.of(NotifyRequest)` → principal của target; `NotifyRules.violations(NotifyRequest) -> List<String>` (rỗng = hợp lệ); `NotifyCases.valid()`, `NotifyCases.invalid()` (`record Case(String name, NotifyRequest request, String rule)`).

- [ ] **Step 1: Write the failing test** `NotifyRulesTest`:
  - `validCasesHaveNoViolations`: mọi `NotifyCases.valid()` → `violations` rỗng.
  - `eachInvalidCaseBreaksItsRule`: mỗi case invalid → `violations` chứa `case.rule()`; phải có đủ các rule `request_id`, `target`, `expires`, `alert`, `alert_size` (thân Alert > 2 048 byte).
  - `keyIsTheRecipient`: `NotifyKeys.of(request với device {p1, d1})` = `"p1"`.
- [ ] **Step 2:** `./mvnw -q -f platform-contracts-testkit test -Dtest=NotifyRulesTest` → FAIL (compile).
- [ ] **Step 3:** Thêm proto đúng spec §2.1/§2.2, `ChannelNotifyJob`, các lớp testkit. `cd platform-contracts && buf lint && buf breaking --against '.git#branch=main,subdir=platform-contracts'` (như CI `contracts-lint`).
- [ ] **Step 4:** `./mvnw -q -f platform-contracts install && ./mvnw -q -f platform-contracts-testkit install` → PASS.
- [ ] **Step 5: Commit** `feat(contracts): NotifyRequest on cN.notify.requests, AccountState.display_name, ChannelNotifyJob`.

---

### Task 2: notify-service — khung, token store, API đăng ký

**Files:**
- Create: module `notify-service/` theo khuôn `account-service/` (pom kế thừa `platform-parent`, Jib image `pangate/notify-service:dev`, `application.yml` port 8080 public / 8081 admin+actuator, Flyway `classpath:db/notify`, schema `notify`, `search_path TO notify, public`).
- Create: `src/main/resources/db/notify/V1__init.sql` (bảng và unique index đúng spec §5.2).
- Create: `com.pangate.notify.domain.{Platform, PushToken, TokenRules, DomainException}`; `application.{Caller, Registrations}`; `application.port.{TokenStore, RateLimiter, StoreUnavailableException}`; `infrastructure.postgres.JdbcTokenStore`; `infrastructure.redis.RedisRateLimiter`; `infrastructure.auth.{ConnectTokenVerifier, Revocations, RevocationListener, HttpJwksSource, JwksSource, InvalidTokenException}` (chép từ account-service, sửa cho `Caller` dưới đây); `api.{DevicesController, ConnectAuthFilter, ProblemHandler, PortRoutingFilter}`; `config.{NotifyConfig, NotifyProperties, PersistenceConfig}`; `tools.HealthCheck`.
- Modify: `Makefile` (`MODULES` thêm `notify-service` sau `account-service`; `images` thêm jib của notify-service); `platform-devenv/docker-compose.yml` (service `notify` profile `app`, ports `8090:8080`, `8091:8081`, phụ thuộc postgres/redis/kafka-init/auth, healthcheck như account).
- Test: `src/test/.../domain/TokenRulesTest.java`, `src/it/.../PushTokenStoreIT.java`, `src/it/.../RegistrationApiIT.java`

**Interfaces:**
- Produces: `enum Platform { APNS, FCM }` (JSON `"apns"`/`"fcm"`); `record PushToken(String tenantId, String principalId, String deviceId, Platform platform, String environment, String token, Instant updatedAt)`; `TokenRules.validate(Platform p, String token, String environment)` ném `DomainException("invalid_request", msg)`; `record Caller(String tenantId, String principalId, String deviceId, String jti)`; `ConnectTokenVerifier.verify(String bearer) -> Caller`.
- Produces `TokenStore`: `void register(PushToken t)` (một transaction: xoá dòng khác có cùng `(platform, token)`, rồi upsert theo khoá chính); `void remove(String tenant, String principal, String device)`; `List<PushToken> byPrincipal(String tenant, String principal)`; `Optional<PushToken> byDevice(String tenant, String principal, String device)`; `int removeDevice(String principal, String device)` (mọi tenant); `int removePrincipal(String principal)`; `boolean removeIfToken(String tenant, String principal, String device, String token)`; `int purgeIdle(Instant before)`.
- Produces `Registrations`: `register(Caller c, Platform p, String token, String environment)`, `unregister(Caller c)`; `RateLimiter.tryAcquire(String key, int perMinute) -> boolean`.

- [ ] **Step 1: Write the failing tests:**
  - `TokenRulesTest`: APNs 63 hex → invalid, 64 hex → ok, 201 → invalid, ký tự không hex → invalid; FCM 4 096 → ok, 4 097 → invalid; `environment` khác `production`/`sandbox` với APNs → invalid; FCM với `sandbox` → invalid.
  - `PushTokenStoreIT` (Testcontainers Postgres): `registerThenLookupByPrincipalAndDevice`; `reRegisterReplacesTheDevicesToken`; **`sameTokenMovesToTheNewDevice`** (token T của (chat, chat:a, d1); đăng ký T cho (chat, chat:b, d2) → `byPrincipal(chat, chat:a)` rỗng); `removeIfTokenKeepsANewerToken`; `removeDeviceAcrossTenants`; `removePrincipal`; `purgeIdleRemovesOnlyOldRows`.
  - `RegistrationApiIT` (Spring Boot test + Postgres + Redis + Kafka + JWKS giả như `AccountIT`): `PUT /v1/devices/self/push` → 204 và dòng có tenant/principal/device lấy từ token; thân sai → 400 `invalid_request`; không token → 401 `invalid_token`; lần 11 trong một phút → 429 `rate_limited`; `DELETE` → 204 cả khi chưa có; **`revokedDeviceCannotRegister`** (produce `Revoke{device}` lên `gw.control`, chờ listener bắt kịp, `PUT` → 401 và không có dòng).
- [ ] **Step 2:** `./mvnw -q -f notify-service verify` → FAIL (compile).
- [ ] **Step 3:** Hiện thực. Rate limit Redis: `INCR notifyreg:{principal}/{device}` + `EXPIRE 60` lần đầu; Redis lỗi → cho qua. Postgres lỗi → 503 `unavailable`. `/ready` gồm db và verifier đã nạp JWKS (như account).
- [ ] **Step 4:** `./mvnw -q -f notify-service verify` → PASS.
- [ ] **Step 5: Commit** `feat(notify): notify-service with push token registration per device`.

---

### Task 3: notify-service — thu hồi và dọn token

**Files:**
- Create: `application.TokenRevoker`, `infrastructure.kafka.RevocationDeleter`, `application.PurgeIdleTokens` (job hằng ngày, `@Scheduled` trong `config.JobsConfig`, khoá advisory như account `ExpireSessions`)
- Test: `src/test/.../application/TokenRevokerTest.java`, `src/it/.../RevocationDeleterIT.java`

**Interfaces:**
- Consumes: `TokenStore.removeDevice/removePrincipal/purgeIdle` (Task 2).
- Produces: `TokenRevoker.apply(ControlEvent e) -> int` (số token xoá; `jti`, `client_id`, sự kiện khác → 0).

- [ ] **Step 1: Write the failing tests:**
  - `TokenRevokerTest` (TokenStore giả): `Revoke{device}` → `removeDevice(principal, device)`; `Revoke{principal}` → `removePrincipal`; `jti`/`client_id`/`CellMap` → không gọi gì.
  - `RevocationDeleterIT` (Kafka + Postgres): token của (chat:a, d1) và (chat:a, d2); produce `Revoke{device chat:a/d1}` → còn d2; `Revoke{principal chat:a}` → hết; restart deleter với group `notify-revocations` không xoá lại gì (offset đã commit); bản ghi hỏng bị bỏ, loop không chết.
  - `PurgeIdleTokensTest`: cắt ở `now − 180 ngày`.
- [ ] **Step 2:** `./mvnw -q -f notify-service verify -Dit.test=RevocationDeleterIT -Dtest='TokenRevokerTest,PurgeIdleTokensTest'` → FAIL.
- [ ] **Step 3:** Hiện thực; consumer group `notify-revocations`, `auto.offset.reset=earliest`, commit sau khi xoá; Postgres lỗi → không commit, lùi dần (100 ms → 5 s), thread không chết. Metric `notify.tokens.removed{reason=revoked|idle}`.
- [ ] **Step 4:** chạy lại → PASS.
- [ ] **Step 5: Commit** `feat(notify): drop push tokens of revoked devices and principals, purge idle ones`.

---

### Task 4: devtools — APNs và FCM giả, lệnh `notify register`

**Files:**
- Create: `gateway-devtools/src/main/java/com/pangate/devtools/cmd/{FakeApnsCommand, FakeFcmCommand, NotifyCommand}.java`, `gateway-devtools/src/main/java/com/pangate/devtools/push/FakeProviderLog.java`
- Modify: `gateway-devtools/.../Main` (đăng ký lệnh), `platform-devenv/docker-compose.yml` (`fake-apns` cổng 9550, `fake-fcm` cổng 9551, image devtools, profile `app`)
- Test: `gateway-devtools/src/test/java/com/pangate/devtools/cmd/FakeProvidersTest.java`

**Interfaces:**
- Produces: `fake-apns --port` (h2c; `POST /3/device/{token}`; kiểm header `authorization: bearer <JWT ES256>` có `kid`/`iss`, `apns-topic`, `apns-push-type`; trả 200, hoặc 410 `{"reason":"Unregistered"}` khi token bắt đầu bằng `dead`, 429 khi bắt đầu bằng `busy` (lần đầu)); `fake-fcm --port` (`POST /token` trả `{"access_token":"fake","expires_in":3600}`; `POST /v1/projects/{p}/messages:send` kiểm `Bearer fake`, trả 200 `{"name":...}`, 404 `{"error":{"status":"NOT_FOUND","details":[{"errorCode":"UNREGISTERED"}]}}` cho token `dead...`); cả hai `GET /received?token=<t>` → mảng JSON các thân đã nhận (kèm header đáng chú ý). `notify register --url --token-file|--key --platform --push-token [--environment]`.

- [ ] **Step 1: Write the failing test** `FakeProvidersTest`: khởi động hai fake trên cổng ngẫu nhiên; gửi request APNs hợp lệ → 200 và `/received` có thân; token `dead1` → 410 `Unregistered`; FCM lấy token rồi gửi → 200; `dead2` → 404 `UNREGISTERED`; thiếu `authorization` → 403.
- [ ] **Step 2:** `./mvnw -q -f gateway-devtools test -Dtest=FakeProvidersTest` → FAIL.
- [ ] **Step 3:** Hiện thực bằng `com.sun.net.httpserver` cho FCM; APNs cần HTTP/2 cleartext (h2c prior knowledge): dùng Netty (đã là phụ thuộc của devtools) với `Http2FrameCodec`.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(devtools): fake APNs and FCM for notify tests, notify register command`.

---

### Task 5: notify-service — gửi thông báo

**Files:**
- Create: `application.{NotifyDispatcher, SendResult}`, `application.port.{PushProvider, Dedup}`, `infrastructure.apns.{ApnsProvider, ApnsJwt, ApnsPayload}`, `infrastructure.fcm.{FcmProvider, FcmAccessToken, FcmPayload}`, `infrastructure.redis.RedisDedup`, `infrastructure.kafka.NotifyRequestLoop`
- Modify: `config.NotifyProperties` (`tenants.<tenant>.apns{base-url, key-file, key-id, team-id, bundle-id}`, `tenants.<tenant>.fcm{base-url, token-uri, project-id, service-account-file}`, `max-in-flight: 1000`, `retries: 3`), devenv: notify trỏ `fake-apns`/`fake-fcm`, sinh khoá `.p8` và service account giả vào volume `dev-secrets` (như auth sinh khoá).
- Test: `src/test/.../application/NotifyDispatcherTest.java`, `src/test/.../infrastructure/apns/ApnsPayloadTest.java`, `src/test/.../infrastructure/fcm/FcmPayloadTest.java`, `src/it/.../SendIT.java`

**Interfaces:**
- Consumes: `TokenStore.byPrincipal/byDevice/removeIfToken` (Task 2); fake providers (Task 4); `NotifyRules` (Task 1).
- Produces: `sealed interface SendResult { Ok; TokenDead; Transient(long retryAfterMs); AuthFailed }`; `PushProvider.send(PushToken t, NotifyRequest r) -> CompletableFuture<SendResult>`; `Dedup.firstTime(String requestId) -> boolean` (Redis lỗi → true); `NotifyDispatcher.handle(NotifyRequest r, long nowMs) -> CompletableFuture<String>` (kết quả cho metric `notify.requests{result}`: `sent`, `no_token`, `expired`, `duplicate`, `invalid`, `no_config`).

- [ ] **Step 1: Write the failing tests:**
  - `NotifyDispatcherTest` (store, dedup, provider, đồng hồ giả): `sendsToEveryTokenOfThePrincipal`; `deviceTargetSendsToOneDevice`; `expiredIsDropped`; `invalidIsDropped`; **`duplicateRequestIsSentOnce`**; `dedupDownStillSends`; `noTokenIsCounted`; `tenantWithoutConfigIsCounted`; `deadTokenIsRemoved`; **`deadTokenOnlyRemovedIfStillCurrent`** (`removeIfToken` trả false khi token trong DB đã đổi → không xoá token mới); `transientRetriedThreeTimesHonoringRetryAfter` (chờ 1 s, 4 s khi không có `Retry-After`); `retryNeverPastExpiry`; `authFailedNotRetried`.
  - `ApnsPayloadTest`: `aps.alert.title/body`; `loc_key` → `aps.alert.loc-key`, `loc-args`; `badge = -1` → không có `badge`; `thread-id`; `data` ở gốc; header `apns-collapse-id`, `apns-expiration` (giây), `apns-priority` 10/5.
  - `FcmPayloadTest`: `notification{title, body}` hoặc `android.notification.body_loc_key`; `android.notification.tag`, `android.collapse_key`, `android.ttl` (giây, `"<n>s"`), `android.priority` HIGH/NORMAL, `notification_count` chỉ khi `badge >= 0`; `data`.
  - `SendIT` (Postgres + Redis + Kafka + fake providers trong JVM): produce `NotifyRequest` cho principal có 1 token APNs và 1 token FCM → mỗi fake nhận đúng một; token `dead...` bị xoá khỏi DB; produce lại cùng `request_id` → không nhận thêm.
- [ ] **Step 2:** `./mvnw -q -f notify-service verify -Dtest='NotifyDispatcherTest,ApnsPayloadTest,FcmPayloadTest' -Dit.test=SendIT` → FAIL.
- [ ] **Step 3:** Hiện thực. `ApnsJwt`: ES256 bằng Nimbus, `iat` hiện tại, cache 50 phút. `FcmAccessToken`: JWT RS256 của service account → `POST token-uri` (grant `urn:ietf:params:oauth:grant-type:jwt-bearer`), cache tới `expires_in − 60 s`. Một `HttpClient` HTTP/2 mỗi provider. `NotifyRequestLoop`: group `notify`, poll tới 500, gửi song song dưới semaphore `max-in-flight`, commit offset khi mọi yêu cầu của batch xong (store lỗi → tua lại về offset đã commit, lùi dần, như `ReceiptWorkerLoop` của chat).
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(notify): deliver NotifyRequest to APNs and FCM, drop dead tokens, dedup by request id`.

---

### Task 6: Tên hiển thị và schema V5 của chat

**Files:**
- Modify: `account-service/.../application/Profiles.java` (nhận thêm `Outbox` và `Transactions` qua constructor; `update` có đổi tên thì ghi `updateProfile` và outbox `event(userId, ACTIVE)` trong cùng transaction), `config/AccountConfig.java` (truyền hai phụ thuộc đó), `account-service/.../infrastructure/postgres/JdbcOutbox.java` (`event` đọc `display_name` của user trong cùng transaction, đặt vào `AccountState.display_name`)
- Create: `chat-service/src/main/resources/cql/V5__notifications.cql`:
  `ALTER TABLE users ADD display_name text`; `ALTER TABLE members ADD muted_until bigint`; `ALTER TABLE user_convs ADD muted_until bigint`; `ALTER TABLE user_settings ADD hide_preview boolean`; `CREATE TABLE channel_notify_progress (conv_id text PRIMARY KEY, conv_seq bigint, last_user_id text) WITH default_time_to_live = 86400`.
- Modify: `chat-service/.../store/SchemaMigrator.java` (`MIGRATIONS` + V5), `ChatStores.UserRecord`/`UserStore.put`, `ScyllaChatStore`, `kafka/AccountsListener.java`
- Test: `account-service/src/test/.../ProfilesTest.java` (thêm), `account-service/src/it/.../EventRelayIT.java` (thêm), `chat-service/src/it/.../store/SchemaMigratorIT.java` (expect 5), `chat-service/src/test/.../kafka/AccountsListenerTest.java` (thêm)

**Interfaces:**
- Produces: `ChatStores.UserRecord(String userId, String status, String displayName, long updatedAtMs)`; `UserStore.put(String userId, String status, String displayName, long updatedAtMs)`; `UserStore.displayName(String userId) -> Optional<String>`.

- [ ] **Step 1: Write the failing tests:** `ProfilesTest.renameEmitsAccountEvent` (outbox giả nhận `event` khi `displayName != null`, không nhận khi chỉ đổi bio); `EventRelayIT.eventCarriesDisplayName` (đổi tên → bản ghi `account.users` có `display_name`); `AccountsListenerTest.storesDisplayName` và `olderStateDoesNotOverwriteName`; `SchemaMigratorIT` expect 5 migration.
- [ ] **Step 2:** `./mvnw -q -f account-service verify -Dtest=ProfilesTest -Dit.test=EventRelayIT` và `./mvnw -q -f chat-service test -Dtest=AccountsListenerTest` → FAIL.
- [ ] **Step 3:** Hiện thực.
- [ ] **Step 4:** `./mvnw -q -f account-service verify` và `./mvnw -q -f chat-service verify -Dit.test='SchemaMigratorIT,ChatStoreIT'` → PASS.
- [ ] **Step 5: Commit** `feat(account,chat): account events carry the display name; chat V5 schema for notifications`.

---

### Task 7: chat-service — tắt thông báo và ẩn xem trước

**Files:**
- Create: `chat-service/.../core/Mutes.java`
- Modify: `core/ChatStores.java` (`MuteStore`, `Privacy` thêm `hidePreview`, `SettingsStore.updatePrivacy` thêm tham số), `store/ScyllaChatStore.java`, `http/StateController.java` (`hidePreview` trong `GET/PUT /v1/me/privacy`), `http/QueryController.java` (danh sách hội thoại trả `mutedUntil`), controller mới `http/MuteController.java`
- Test: `core/MutesTest.java`, `http/MuteControllerTest.java`, `it/.../ChatStoreIT.java` (thêm)

**Interfaces:**
- Produces: `Mutes.muted(long mutedUntil, long nowMs) -> boolean` (`mutedUntil > nowMs`); `Mutes.shouldNotify(long mutedUntil, List<String> mentions, String userId, long nowMs) -> boolean`; `MuteStore.setMute(String convId, String userId, long mutedUntil)` (ghi `members` và `user_convs`); `MuteStore.mutedUntil(String convId, String userId) -> long`; `MuteStore.membersPage(String convId, String afterUserId, int limit) -> List<MemberMute>` với `record MemberMute(String userId, long mutedUntil)`; `ChatStores.Privacy(boolean hidePresence, boolean hideRead, boolean hidePreview)`; `SettingsStore.updatePrivacy(String userId, Boolean hidePresence, Boolean hideRead, Boolean hidePreview, long atMs)`.

- [ ] **Step 1: Write the failing tests:** `MutesTest` (0 → không tắt; quá khứ → không; `Long.MAX_VALUE` → tắt; tắt nhưng có tên trong `mentions` → vẫn báo); `MuteControllerTest` (`PUT .../mute {"until":null}` → 204 và `Long.MAX_VALUE`; `{"until": <quá khứ>}` → bật lại; không phải thành viên → 403 `forbidden`; conv không phải ULID → 400); `ChatStoreIT.muteIsReadFromMembersAndUserConvs`, `membersPageIsOrderedAndResumable`, `privacyHidePreviewRoundTrip`.
- [ ] **Step 2:** `./mvnw -q -f chat-service test -Dtest='MutesTest,MuteControllerTest'` → FAIL.
- [ ] **Step 3:** Hiện thực. `Privacy` thêm trường thứ ba: sửa mọi chỗ tạo `Privacy` của CH3 (`PrivacyRules`, `PrivacyCache`, `ScyllaChatStore`, fake và test) sang ba tham số.
- [ ] **Step 4:** `./mvnw -q -f chat-service verify -Dit.test=ChatStoreIT` và cả unit suite → PASS.
- [ ] **Step 5: Commit** `feat(chat): mute a conversation, hide notification previews`.

---

### Task 8: chat-service — thông báo cho DM và nhóm

**Files:**
- Create: `core/{NotificationComposer, Badge, OfflineNotifications, NotifySink}.java`, `kafka/{OfflineNotifierLoop, NotifyProducer}.java`
- Modify: `config/ChatConfig.java`, `config/ChatProperties.java` (`notify.offline-topic: c1.push.offline`, `notify.requests-topic: c1.notify.requests`, `notify.max-age: 1h`, `notify.channel-rate: 5000`), `it/.../support/ChatHarness.java`, `ChatStack.java` (topic `c1.push.offline`, `c1.notify.requests`)
- Test: `core/NotificationComposerTest.java`, `core/BadgeTest.java`, `core/OfflineNotificationsTest.java`, `it/.../kafka/OfflineNotifierIT.java`

**Interfaces:**
- Consumes: `Mutes`, `MuteStore`, `SettingsStore`/`PrivacyCache` (Task 7), `UserStore.displayName` (Task 6), `NotifyRequest` (Task 1).
- Produces: `NotificationComposer.compose(ChatMessage m, ConversationKind kind, String convTitle, String senderName, boolean hidePreview, int badge, String recipientUserId, String tenantId) -> NotifyRequest` (đúng bảng spec §3.5; kênh dùng `collapse_id = conversationId`); `Badge.total(String userId) -> int`; `OfflineNotifications.handle(OfflineRecord r, long nowMs) -> Result` với `record Result(String outcome, Optional<NotifyRequest> request)` (`outcome` ∈ `sent`, `muted`, `stale`, `not_chat`, `invalid`); `NotifySink.send(NotifyRequest r) -> CompletableFuture<Void>`.

- [ ] **Step 1: Write the failing tests:**
  - `NotificationComposerTest`: DM title = tên người gửi, body = tin; nhóm title = tên nhóm, body `"Lan: ..."`; kênh title = tên kênh, `collapse_id = convId`; 201 code point → 200 + "…", không cắt đôi emoji (cặp surrogate); ẩn xem trước → title/body trống, `loc_key = "notif_new_message"`; tin không chữ → `loc_key`; `request_id = "<msgId>/<userId>"`; `expires_at_ms = sentAt + 3 600 000`; `data` có đủ ba khoá.
  - `BadgeTest`: tổng `max(0, last − read)`, bỏ hội thoại đã rời, gồm hội thoại đã tắt.
  - `OfflineNotificationsTest`: tenant khác `chat` → `not_chat`; header `type` khác `chat.message` → `not_chat`; `sentAt` cũ hơn 1 giờ → `stale`; đang tắt → `muted`; đang tắt nhưng được nhắc tên → `sent`; thiếu tên hiển thị → dùng `userId`.
  - `OfflineNotifierIT` (Kafka + Scylla): produce `OfflineRecord` cho tin DM của a tới b → đọc được một `NotifyRequest` đúng title/body/badge trên `c1.notify.requests`, key `chat:b`; **`replayedOfflineRecordNotifiesOnce`** (produce lại cùng bản ghi → hai `NotifyRequest` có `request_id` giống hệt nhau, nên notify-service chỉ gửi một lần; phần dedup đã được test ở Task 5); store lỗi tạm → loop không chết, bản ghi được xử lý sau.
- [ ] **Step 2:** `./mvnw -q -f chat-service test -Dtest='NotificationComposerTest,BadgeTest,OfflineNotificationsTest'` → FAIL.
- [ ] **Step 3:** Hiện thực. `OfflineNotifierLoop`: group `chat-notify`, commit sau khi `NotifySink` được Kafka ack; lỗi tạm thì tua lại và lùi dần như `ReceiptWorkerLoop`. Cache tên 60 s theo pod. Metric `chat.notify.offline{result}`.
- [ ] **Step 4:** `./mvnw -q -f chat-service verify -Dit.test=OfflineNotifierIT` và unit suite → PASS.
- [ ] **Step 5: Commit** `feat(chat): notify offline DM and group recipients through notify-service`.

---

### Task 9: chat-service — thông báo cho kênh

**Files:**
- Create: `core/ChannelNotifyPlan.java`, `kafka/ChannelNotifierLoop.java`, `kafka/ChannelNotifyJobs.java` (producer của job)
- Modify: `kafka/FanOutLoop.java` (bài kênh đã lưu → produce `ChannelNotifyJob`, lỗi produce chỉ đếm metric, không chặn fan-out), `core/PresenceState.java` + `store/RedisPresence.java` (`onlineOf`), `ChatStores` + `ScyllaChatStore` (`ChannelProgressStore`), `config/ChatConfig.java`
- Test: `core/ChannelNotifyPlanTest.java`, `it/.../kafka/ChannelNotifierIT.java`, `it/.../store/RedisPresenceIT.java` (thêm `onlineOfPipelinesAPage`)

**Interfaces:**
- Consumes: `MuteStore.membersPage` (Task 7), `NotificationComposer`, `NotifySink` (Task 8), `TokenBucketLimiter` (đã có trong core).
- Produces: `PresenceState.onlineOf(Collection<String> userIds) -> Set<String>`; `ChannelProgressStore.save(String convId, long convSeq, String lastUserId)`, `ChannelProgressStore.load(String convId) -> Optional<Progress>` với `record Progress(long convSeq, String lastUserId)`; `ChannelNotifyPlan.recipients(List<MemberMute> page, Set<String> online, ChatMessage m, long nowMs) -> List<String>` (bỏ người gửi, người online, người đang tắt mà không được nhắc tên).

- [ ] **Step 1: Write the failing tests:**
  - `ChannelNotifyPlanTest`: bỏ người gửi; bỏ người online; bỏ người đang tắt; giữ người đang tắt được nhắc tên; giữ đúng thứ tự trang.
  - `ChannelNotifierIT` (Kafka + Scylla + Redis): kênh 2 500 người theo dõi, 3 người online, 2 người tắt → đúng 2 495 `NotifyRequest`, `badge = -1`, `collapse_id = convId`; **bài mới thay job cũ** (job bài 1 đang chạy, job bài 2 tới → không ai nhận cả hai bài; tổng ≤ số người theo dõi + 1 trang); **`resumesFromProgressAfterRestart`** (dừng loop sau 1 trang đã lưu điểm dừng, chạy loop mới → các `request_id` không lặp ngoài trang cuối, và mọi người theo dõi đều có đúng một yêu cầu sau khi lọc trùng `request_id`); bài cũ hơn 1 giờ → không đẩy; giới hạn 5 000/s: 2 495 yêu cầu ở rate 1 000/s mất ≥ 2 s.
  - `RedisPresenceIT.onlineOfPipelinesAPage`.
- [ ] **Step 2:** `./mvnw -q -f chat-service test -Dtest=ChannelNotifyPlanTest` → FAIL.
- [ ] **Step 3:** Hiện thực. Loop group `chat-channel-notify`: giữ trong bộ nhớ job sống theo `convId` (job mới hơn theo `convSeq` thay job cũ, ghi điểm dừng `(convId, newSeq, "")`); xoay vòng từng trang giữa các job sống; sau mỗi trang `save` điểm dừng; commit offset nhỏ nhất của các job chưa xong mỗi partition (như `ReceiptWorkerLoop.commit`); khi được giao partition, job đọc lại dùng `load` để đi tiếp nếu `convSeq` khớp. Metric `chat.notify.channel.jobs{result}`, `chat.notify.channel.sent`.
- [ ] **Step 4:** `./mvnw -q -f chat-service verify -Dit.test='ChannelNotifierIT,RedisPresenceIT,FanOutIT'` → PASS.
- [ ] **Step 5: Commit** `feat(chat): notify offline channel followers, one live job per channel, resumable`.

---

### Task 10: E2E, smoke, đo, tài liệu

**Files:**
- Create: `chat-service/src/it/java/com/pangate/chat/it/ChatNotifyE2EIT.java` (gateway thật, Push API, router, notify-service trong JVM test như `ChatStateE2EIT`, fake APNs của devtools)
- Modify: `gateway-devtools/.../cmd/ChatSmoke.java` (bước `CHAT notify`), `SmokeCommand.java` (tuỳ chọn `--notify-url`, `--fake-apns-url`, dòng OK thêm `chat notify`), `SmokeCommandTest.java`
- Create: `notify-service/README.md`, `docs/adr/0040-notify-service-split.md`, `docs/adr/0041-channel-notification-jobs.md`; Modify: `docs/adr/README.md`, `chat-service/README.md`, `CHANGELOG.md`, spec (trạng thái "đã hiện thực" + số đo)
- Create: `chat-service/src/it/java/com/pangate/chat/measure/ChannelNotifyMeasureIT.java` (`@Tag("measure")`, không chạy trong `verify`)

- [ ] **Step 1: Write the failing tests:** `ChatNotifyE2EIT`: `onlineRecipientGetsNoNotification`; `offlineRecipientGetsTitleBodyAndBadge`; `mutedConversationIsSilentUnlessMentioned`; `hidePreviewSendsNoContent`; `loggedOutDeviceGetsNothing` (đăng xuất qua account-service giả lập bằng `Revoke{device}` trên `gw.control`); `SmokeCommandTest` OK line có `chat notify`.
- [ ] **Step 2:** `./mvnw -q -f chat-service verify -Dit.test=ChatNotifyE2EIT` → FAIL.
- [ ] **Step 3:** Hiện thực bước smoke: b đăng nhập, `notify register` token APNs giả `smoke<hex>`, ngắt; a gửi DM; chờ `/received` của fake-apns có đúng một thân với title tên a, body là tin, `badge` ≥ 1 (≤ 10 s); b đăng xuất thiết bị, a gửi tin nữa, sau 5 s `/received` vẫn một.
- [ ] **Step 4:** `./mvnw -q -f chat-service verify` (cả suite), `./mvnw -q -f notify-service verify`, `./mvnw -q -f gateway-devtools verify`, `./mvnw -q -f account-service verify`; `make images up smoke down` → `SMOKE OK (... chat notify ...)`. Đo: `./mvnw -q -f chat-service verify -Dit.test=ChannelNotifyMeasureIT -Dgroups=measure` (100 000 người theo dõi offline, fake APNs) → ghi thời gian vào `chat-service/README.md`.
- [ ] **Step 5: Commit** `feat(chat,devtools): notification E2E and smoke step; docs: ADR 0040-0041`.
