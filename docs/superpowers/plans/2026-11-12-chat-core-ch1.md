# chat-service CH1 (Chat core) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Module mới `chat-service`: hội thoại 1-1, nhóm, kênh; gửi tin qua WebSocket (upstream `cmd`) và HTTP; một nơi ghi mỗi hội thoại cấp `convSeq` trên ScyllaDB; fan-out xuống client qua Push API; API lịch sử và đồng bộ.

**Architecture:** Ingest (upstream `cmd` + HTTP) → `chat.commands` (key `conversationId`) → Sequencer (chống trùng, LWT `last_seq`, ghi ScyllaDB) → `chat.events` → FanOut (gRPC `SendStream` của Push API: MESSAGE theo thành viên, BROADCAST theo topic kênh). Lõi Java thuần trong `core/`, adapter trong `store/`, `kafka/`, `grpc/`, `http/`, nối trong `config/`, như `push-api-service`.

**Tech Stack:** Java 21, Spring Boot 3.5.16 (MVC, virtual threads), ScyllaDB driver `com.scylladb:java-driver-core:4.19.2.3`, Kafka clients, Lettuce, gRPC 1.85.1, Nimbus JOSE, Testcontainers 1.21.4 (`scylladb`, `kafka`), image `scylladb/scylla:6.2.3`.

**Spec:** `docs/superpowers/specs/2026-11-12-chat-core-design.md`

## Global Constraints

- Không sửa code gateway, router, auth, Push API; chỉ thêm cấu hình (spec §2).
- Contracts chỉ thêm: `platform-contracts/proto/pangate/v1/chat.proto`, package `pangate.v1`; `buf breaking` xanh (spec §3).
- Tenant PanGate cố định `chat`; principal `chat:<userId>`; principal không có tiền tố `chat:` → `invalid` (spec §1).
- `clientMsgId`, `conversationId`, `msgId` là ULID; `text` 1..4096 byte UTF-8; tối đa 50 mention (spec §4.3).
- Giới hạn: user 20 tin/s burst 40; hội thoại 50 tin/s burst 100; nhóm tối đa 500 thành viên (spec §4.3, §7).
- Topic: `chat.commands` (64), `chat.events` (64), `chat.commands.dlq` (8); consumer group `chat-ingest`, `chat-sequencer`, `chat-fanout` (spec §4.4, §6, §8).
- `bucket = conv_seq / 10 000`; `send_dedup` TTL 24 h; cache thành viên TTL 60 s; ScyllaDB timeout 1 s; HTTP chờ kết quả 3 s (spec §4.2, §5).
- Kênh: topic `chat:chan:<convId>`, BROADCAST EPHEMERAL; nhóm/1-1: MESSAGE RELIABLE tới `chat:<userId>`, `headers.type=chat.message`, không `idempotency_key` (spec §6).
- Connect token: `aud=pangate-gw`, `iss`, lệch đồng hồ 30 s, claim `sub` (principal), `tenant`, `device`, `jti` (spec §1).
- Lỗi HTTP: `application/problem+json`, `code` ∈ `invalid_request`, `not_found`, `forbidden`, `rate_limited`, `unavailable`, `invalid_token` (spec §7).
- Profile `local`: HTTP 8086, quản trị 8087; `dev`: 8080/8081 (host 8086/8087) (spec §8).
- Mỗi commit kết thúc bằng hai dòng trailer của session (Co-Authored-By, Claude-Session).

## Review Focus

1. **Tin chứa emoji / ký tự nhiều byte ở biên 4096:** giới hạn tính theo **byte UTF-8**, không theo ký tự: 1024 emoji 4 byte hợp lệ, thêm 1 byte là `too_long`. → Task 4, `SendValidatorTest.textLimitIsUtf8Bytes`.
2. **User vừa bị xoá khỏi nhóm vẫn nhận tin tới 60 s** vì cache thành viên: xoá thành viên phải xoá cache ngay; tin gửi sau đó không tới người bị xoá. → Task 7, `QueryApiIT.removedMemberStopsReceivingAtOnce`.
3. **Client gọi `/v1/sync` với `lastSeq` lớn hơn `last_seq` của hội thoại** (dữ liệu client hỏng, hay hội thoại đã bị tạo lại): trả rỗng, không lỗi, không `tooMany`. → Task 7, `SyncTest.clientAheadGetsNothing`.
4. **Phân trang lịch sử ngay biên bucket** (`before=10000`, `after=9999`): không mất, không lặp tin. → Task 3, `ChatStoreIT.pagingAcrossBucketEdgeHasNoGapOrRepeat`.
5. **Cùng `clientMsgId` dùng cho hai hội thoại khác nhau:** là hai tin khác nhau (khoá chống trùng gồm `conversationId`). → Task 5, `SequencerTest.sameClientMsgIdInTwoConversationsIsTwoMessages`.

---

## File Structure

```
platform-contracts/proto/pangate/v1/chat.proto                     (Task 1)
platform-contracts-testkit/src/main/java/com/pangate/contracts/testkit/ChatTopics.java, ChatSendCases.java  (Task 1)
chat-service/pom.xml, src/main/resources/{chat.yml,chat-local.yml,chat-dev.yml,cql/V1__schema.cql}          (Task 2)
chat-service/src/main/java/com/pangate/chat/
  ChatApplication.java
  config/  ChatProperties, ChatConfig, AdminConnector, ChatHealth                     (Task 2, mở rộng dần)
  core/    Ids, UserIds, ConversationKind, SendInput, SendValidator, Rejection, TokenBucketLimiter,
           Ingest, CommandSink, Sequencer, SequencedMessage, ChatStores (interfaces), FanOutPlanner,
           Pusher, SyncService                                                       (Task 4–8)
  store/   ScyllaSession, SchemaMigrator, ScyllaChatStore, RedisChatCache            (Task 2–3)
  kafka/   UpstreamCommandListener, CommandProducer, SequencerLoop, EventProducer, FanOutLoop  (Task 4–6)
  grpc/    PushApiClient                                                              (Task 6)
  auth/    ConnectTokenVerifier, JwksSource, RevocationListener                       (Task 4)
  http/    Problems, PortRoutingFilter, AuthFilter, SendController, QueryController   (Task 4, 7)
platform-devenv/docker-compose.yml, Makefile, auth-service SeedRunner (seed tenant chat)   (Task 2)
gateway-devtools/src/main/java/com/pangate/devtools/cmd/SmokeCommand.java (bước chat)  (Task 9)
```

---

### Task 1: Contracts `chat.proto` và testkit

**Files:**
- Create: `platform-contracts/proto/pangate/v1/chat.proto` (đúng các message spec §3)
- Create: `platform-contracts-testkit/src/main/java/com/pangate/contracts/testkit/ChatTopics.java`, `ChatSendCases.java`
- Test: `platform-contracts/src/test/java/com/pangate/contracts/ChatContractNumbersTest.java`, `platform-contracts-testkit/src/test/java/com/pangate/contracts/testkit/ChatSendCasesTest.java`

**Interfaces:**
- Produces: proto `ChatSend`, `ChannelOpen`, `ChannelClose`, `ChatMessage`, `ChatRejected`, `ChatCommand`, `ChatEvent`, enum `ConversationKind` (số field đúng spec §3). `ChatTopics.COMMANDS = "chat.commands"`, `EVENTS = "chat.events"`, `DLQ = "chat.commands.dlq"`, `TENANT = "chat"`, `PRINCIPAL_PREFIX = "chat:"`, `static String channelTopic(String convId)` → `"chat:chan:" + convId`. `ChatSendCases.all(): List<Case>` với `Case(String name, ChatSend send, String expectedReason)` (`expectedReason` rỗng = hợp lệ).

- [ ] **Step 1:** `ChatContractNumbersTest.fieldNumbersArePinned`: assert `ChatSend.TEXT_FIELD_NUMBER == 3`, `ChatMessage.CONV_SEQ_FIELD_NUMBER == 3`, `ChatMessage.SENT_AT_MS_FIELD_NUMBER == 9`, `ChatCommand.SEND_FIELD_NUMBER == 10`, `ChatCommand.REPLY_TO_FIELD_NUMBER == 20`, `ConversationKind.CHANNEL_VALUE == 3`. `ChatSendCasesTest.casesCoverEveryRule`: có ít nhất một case cho mỗi reason `invalid`, `too_long`; case `"text 4096 bytes"` hợp lệ, `"text 4097 bytes"` → `too_long`, `"1024 four-byte emoji"` hợp lệ, `"51 mentions"` → `invalid`, `"client_msg_id not ULID"` → `invalid`; `ChatTopics.channelTopic("01J…")` = `"chat:chan:01J…"`.
- [ ] **Step 2:** Chạy `./mvnw -f platform-contracts test -Dtest=ChatContractNumbersTest` và `./mvnw -f platform-contracts-testkit test -Dtest=ChatSendCasesTest`. Expected: FAIL (không biên dịch, chưa có class).
- [ ] **Step 3:** Viết `chat.proto` đúng spec §3; `ChatTopics`; `ChatSendCases` (các case nêu ở Step 1 cùng: `conversation_id` rỗng → `invalid`, text rỗng → `invalid`).
- [ ] **Step 4:** Chạy lại hai lệnh, rồi `./mvnw -f platform-contracts verify` và `-f platform-contracts-testkit install`. Expected: PASS; job `contracts-lint` (buf) sẽ chạy trên CI.
- [ ] **Step 5:** Commit `feat(contracts): chat.proto and chat testkit (topics, ChatSend cases)`.

---

### Task 2: Khung `chat-service`, migration ScyllaDB, devenv

**Files:**
- Create: `chat-service/pom.xml` (theo `push-api-service/pom.xml`; thêm `com.scylladb:java-driver-core:4.19.2.3`, test `org.testcontainers:scylladb`; classifier `exec` ở cấp plugin), `ChatApplication`, `config/ChatProperties`, `config/AdminConnector`, `store/ScyllaSession`, `store/SchemaMigrator`, `resources/chat.yml`, `chat-local.yml`, `chat-dev.yml`, `resources/cql/V1__schema.cql`
- Modify: `platform-devenv/docker-compose.yml` (service `scylla` `scylladb/scylla:6.2.3 --smp 1 --memory 750M --overprovisioned 1`, healthcheck `cqlsh -e 'describe keyspaces'`; topic `chat.*` trong `kafka-init`; service `chat-service` profile `app`); `Makefile` (MODULES thêm `chat-service` sau `push-api-service`; `images`; `chat-local`); `auth-service/.../SeedRunner.java` (seed tenant `chat`, client `chat-backend` với scope `push:send push:broadcast`) và `push-api` dev config `allowed-spiffe` thêm `spiffe://pangate/cell-1/chat-service`; `kafka-init` ghi `TenantCfg` cho `tenantcfg:tenant:chat` lên `gw.control` (`push_rate` 20 000, `push_burst` 40 000, `topicsPerConn` 64) bằng `gateway-devtools control` hoặc `kafka-console-producer`
- Test: `chat-service/src/test/java/com/pangate/chat/config/ChatPropertiesTest.java`, `chat-service/src/it/java/com/pangate/chat/store/SchemaMigratorIT.java`

**Interfaces:**
- Produces: `record ChatProperties(String cell, int managementPort, Kafka kafka, Scylla scylla, Redis redis, Auth auth, PushApi pushApi, Limits limits, Http http)` với `Limits(int userRate, int userBurst, int convRate, int convBurst, int maxGroupMembers, int maxTextBytes, int maxMentions)` mặc định 20/40/50/100/500/4096/50; `Scylla(List<String> contactPoints, String localDc, String keyspace, Duration timeout=1s, int replicationFactor=1)`; `Http(Duration sendWait=3s)`. `ScyllaSession.connect(ChatProperties.Scylla): CqlSession`. `SchemaMigrator.migrate(CqlSession, String keyspace): int` (số migration đã áp dụng lần này).

- [ ] **Step 1:** `ChatPropertiesTest.defaultsMatchSpec` (giá trị mặc định ở trên; `kafka().commandsTopic()` = `chat.commands`). `SchemaMigratorIT.migrationIsIdempotent`: Scylla container; `migrate` lần 1 trả 1, lần 2 trả 0; sáu bảng spec §5 tồn tại (`system_schema.tables`); `send_dedup` có `default_time_to_live = 86400`.
- [ ] **Step 2:** `./mvnw -f chat-service verify`. Expected: FAIL (module mới chưa có code).
- [ ] **Step 3:** Hiện thực. `V1__schema.cql` đúng bảng và khoá spec §5 (`messages` PRIMARY KEY `((conv_id, bucket), conv_seq)` CLUSTERING ORDER `conv_seq DESC`). `SchemaMigrator` ghi bảng `schema_version(version int PRIMARY KEY, applied_at timestamp)` và chạy file `V<n>__*.cql` chưa áp dụng theo thứ tự.
- [ ] **Step 4:** `./mvnw -f chat-service verify`; `docker compose -f platform-devenv/docker-compose.yml config -q`. Expected: PASS.
- [ ] **Step 5:** Commit `feat(chat): chat-service skeleton, ScyllaDB schema migration, devenv`.

---

### Task 3: `ScyllaChatStore` và `RedisChatCache`

**Files:**
- Create: `core/ChatStores.java` (interfaces), `core/ConversationKind.java` (ánh xạ proto), `store/ScyllaChatStore.java`, `store/RedisChatCache.java`
- Test: `chat-service/src/it/java/com/pangate/chat/store/ChatStoreIT.java`

**Interfaces:**
- Produces (trong `ChatStores`):
  - `record Conversation(String id, ConversationKind kind, String title, String createdBy, long lastSeq, boolean isPublic, int memberCount)`
  - `record StoredMessage(String convId, long convSeq, String msgId, String senderId, String clientMsgId, String text, String replyTo, List<String> mentions, long sentAtMs)`
  - `interface ConversationStore { Optional<Conversation> get(String id); void create(Conversation c, List<Member> members); boolean reserveSeq(String convId, long known, int n); long lastSeq(String convId); Optional<String> dm(String userA, String userB); String createDm(String userA, String userB, String newConvId) /* trả convId thắng LWT */; }`
  - `record Member(String userId, Role role)`, `enum Role { OWNER, ADMIN, MEMBER }`
  - `interface MemberStore { List<Member> members(String convId); Optional<Member> member(String convId, String userId); void add(String convId, Member m); void remove(String convId, String userId); List<UserConv> userConvs(String userId, String cursor, int limit); void touchUserConv(String userId, String convId, long seq, long atMs); }`
  - `record UserConv(String convId, ConversationKind kind, long lastSeq, long readSeq, long lastMsgAtMs)`
  - `interface MessageStore { void write(StoredMessage m); List<StoredMessage> before(String convId, long beforeSeq, int limit); List<StoredMessage> after(String convId, long afterSeq, int limit); }`
  - `interface DedupStore { Optional<StoredMessage> find(String convId, String senderId, String clientMsgId); void put(StoredMessage m); }`
  - `RedisChatCache`: `Optional<List<Member>> members(String convId)`, `void putMembers(String convId, List<Member>)`, `void evictMembers(String convId)`, `Optional<StoredMessage> dedup(...)`, `void putDedup(StoredMessage)`; mọi lỗi Redis → trả rỗng / bỏ qua, không ném.
- Mọi lỗi ScyllaDB → `StoreUnavailableException` (core).

- [ ] **Step 1:** `ChatStoreIT`:
  - `reserveSeqIsACompareAndSet`: tạo hội thoại; `reserveSeq(id, 0, 3)` true, `lastSeq` 3; `reserveSeq(id, 0, 1)` false.
  - `pagingAcrossBucketEdgeHasNoGapOrRepeat` (Review Focus 4): ghi `conv_seq` 9 995..10 005; `before(id, 10_001, 5)` = 10 000..9 996 giảm dần; `after(id, 9_999, 5)` = 10 000..10 004 tăng dần.
  - `dmIsUniquePerPair`: hai `createDm(a, b, X)` / `createDm(b, a, Y)` trả cùng một convId.
  - `touchUserConvKeepsTheHighestSeq`: `touch(u, c, 5)` rồi `touch(u, c, 3)` → `lastSeq` 5.
  - `dedupExpiresAfterTtl`: `put` ghi TTL 86 400 (đọc `TTL(msg_id)` > 86 000).
  - `redisDownIsEmptyNotError`: `RedisChatCache` trỏ cổng chết → `members` rỗng, `putMembers` không ném.
- [ ] **Step 2:** `./mvnw -f chat-service verify -Dit.test=ChatStoreIT`. Expected: FAIL.
- [ ] **Step 3:** Hiện thực; `touchUserConv` dùng `USING TIMESTAMP <seq>` để giữ giá trị lớn nhất; `createDm` dùng `INSERT … IF NOT EXISTS` trên `dm_index` với `(min, max)` của hai user.
- [ ] **Step 4:** Chạy lại. Expected: PASS 6/6.
- [ ] **Step 5:** Commit `feat(chat): ScyllaDB and Redis stores`.

---

### Task 4: Ingest, xác thực, đường gửi HTTP (202)

**Files:**
- Create: `core/Ids.java` (ULID sinh/kiểm), `core/UserIds.java`, `core/SendInput.java`, `core/SendValidator.java`, `core/Rejection.java`, `core/TokenBucketLimiter.java`, `core/Ingest.java`, `core/CommandSink.java`, `core/Notifier.java`; `kafka/UpstreamCommandListener.java`, `kafka/CommandProducer.java`; `auth/ConnectTokenVerifier.java`, `auth/JwksSource.java`, `auth/RevocationListener.java`; `http/Problems.java`, `http/PortRoutingFilter.java`, `http/AuthFilter.java`, `http/SendController.java`
- Test: `core/SendValidatorTest`, `core/IngestTest`, `auth/ConnectTokenVerifierTest`, it `kafka/UpstreamCommandListenerIT`

**Interfaces:**
- Consumes: Task 1 `ChatSendCases`, `ChatTopics`; Task 3 `ConversationStore`, `MemberStore`, `RedisChatCache`.
- Produces:
  - `UserIds.fromPrincipal(String principal): Optional<String>` (bỏ `chat:`), `UserIds.principal(String userId): String`.
  - `record SendInput(String senderId, String senderDeviceId, ChatSend send, String replyTo)`.
  - `SendValidator.check(ChatSend s, ChatProperties.Limits l): Optional<Rejection>`; `record Rejection(String reason, String detail)`; reason ∈ `invalid`, `too_long`, `not_found`, `not_member`, `rate_limited`.
  - `TokenBucketLimiter(int rate, int burst, LongSupplier nanos)`, `boolean tryAcquire(String key)`.
  - `Ingest.accept(SendInput in): Optional<Rejection>`: kiểm tra §4.3 theo thứ tự bảng, ghi `ChatCommand` qua `CommandSink.publish(ChatCommand)`.
  - `interface Notifier { void rejected(String senderId, ChatRejected r); }` (hiện thực ở Task 6; Task 4 dùng bản no-op trong test).
  - `ConnectTokenVerifier.verify(String token): Caller` với `record Caller(String userId, String deviceId, String jti)`; ném `InvalidTokenException` / `RevokedTokenException`; cache JWKS và giới hạn tải lại như `push-api` `ApiTokenVerifier` (không chặn khi khoá đã cache).
  - HTTP `POST /v1/conversations/{id}/messages` (header `Idempotency-Key` bắt buộc): Task 4 trả `202 {clientMsgId}`; Task 5 thêm chờ kết quả.

- [ ] **Step 1:** Test:
  - `SendValidatorTest.everyContractCase`: mọi case của `ChatSendCases.all()` cho đúng reason.
  - `SendValidatorTest.textLimitIsUtf8Bytes` (Review Focus 1): `"😀".repeat(1024)` hợp lệ; thêm `"a"` → `too_long`.
  - `IngestTest.notMemberIsRejectedAndNotPublished`, `IngestTest.channelPostNeedsOwnerOrAdmin`, `IngestTest.userLimit20PerSecondBurst40` (41 lệnh liền: 40 qua, lệnh 41 `rate_limited`), `IngestTest.conversationLimit50Burst100`, `IngestTest.validSendPublishesCommandKeyedByConversation` (sink nhận `ChatCommand` có `sender_id` không tiền tố, `send` nguyên vẹn).
  - `ConnectTokenVerifierTest.wrongAudienceIs401`, `.missingJtiIs401`, `.revokedDeviceIs403`, `.principalWithoutChatPrefixIs401`.
  - `UpstreamCommandListenerIT.chatSendFromUpstreamBecomesACommand`: ghi `UpstreamRecord{principal_id="chat:u1", headers{op=chat.send}, payload=ChatSend}` lên `c1.gw.upstream.cmd` → bản ghi trên `chat.commands` key `conversation_id`; record `op=other` bị bỏ qua; payload hỏng → `chat.commands.dlq`.
- [ ] **Step 2:** `./mvnw -f chat-service verify`. Expected: FAIL.
- [ ] **Step 3:** Hiện thực. Metric `chat.send.rejected{reason}` (mỗi `Rejection`). `ChatHealth` (indicator `chat`): ready khi `RevocationListener` đã đọc hết, JWKS đã nạp, và ScyllaDB trả lời trong 10 s gần nhất. `RevocationListener` đọc `gw.control` từ đầu (như `push-api` `ControlListener`), chỉ giữ `revoke:j:*`, `revoke:d:*`, `revoke:p:*`; readiness chờ đọc xong. `UpstreamCommandListener` group `chat-ingest`, commit sau khi `Ingest` xử lý xong mỗi poll; lỗi kiểm tra gọi `Notifier.rejected`.
- [ ] **Step 4:** Chạy lại. Expected: PASS.
- [ ] **Step 5:** Commit `feat(chat): ingest from upstream and HTTP, validation, per-user limits, connect token auth`.

---

### Task 5: Sequencer

**Files:**
- Create: `core/Sequencer.java`, `core/SequencedMessage.java`, `core/SendResults.java` (interface); `kafka/SequencerLoop.java`, `kafka/EventProducer.java`; `store/RedisSendResults.java`
- Modify: `http/SendController.java` (chờ kết quả 3 s → `201`)
- Test: `core/SequencerTest`, it `kafka/SequencerIT`, it `http/SendHttpIT`

**Interfaces:**
- Consumes: Task 3 stores; Task 4 `ChatCommand` trên `chat.commands`.
- Produces:
  - `Sequencer.process(String convId, List<ChatCommand> batch): List<SequencedMessage>` (theo thứ tự lô); `record SequencedMessage(StoredMessage message, boolean duplicate, String replyTo)`; ném `FencedException` khi `reserveSeq` thất bại.
  - `interface SendResults { void publish(String replyTo, StoredMessage m); Optional<StoredMessage> await(String key, Duration wait); }`.
  - `EventProducer.send(ChatEvent e)` (idempotent, `acks=all`, key `conversation_id`).
  - `SequencerLoop`: group `chat-sequencer`; mỗi poll gom theo hội thoại → `process` → ghi sự kiện → `SendResults.publish` cho lệnh có `reply_to` → commit. `FencedException`: `seek` về offset đầu lô của partition đó, xoá `known` trong bộ nhớ.

- [ ] **Step 1:** Test:
  - `SequencerTest.batchGetsConsecutiveSeqs` (3 lệnh, `known=7` → 8, 9, 10; `reserveSeq(id, 7, 3)` gọi đúng một lần).
  - `SequencerTest.duplicateReusesSeqAndIsReEmitted` (dedup có sẵn → `duplicate=true`, cùng `convSeq`, không gọi `reserveSeq` cho nó).
  - `SequencerTest.sameClientMsgIdInTwoConversationsIsTwoMessages` (Review Focus 5).
  - `SequencerTest.fencedWhenSeqMovedElsewhere` (`reserveSeq` trả false → `FencedException`, không ghi tin nào).
  - `SequencerIT.twoSequencersNeverReuseASeq`: hai `SequencerLoop` cùng group, 1 000 lệnh trên 10 hội thoại, ép rebalance giữa chừng (đóng một consumer) → mỗi hội thoại `convSeq` liên tục 1..n, không trùng, số tin = số `clientMsgId` khác nhau.
  - `SequencerIT.crashAfterWriteBeforeCommitKeepsOneMessage`: ghi tin xong, dừng trước commit, chạy lại → vẫn một tin, `chat.events` có hai bản cùng `msg_id`.
  - `SendHttpIT.sendWaitsForResult201` (`201 {msgId, convSeq, sentAt}`), `.sendTimesOut202ThenSameKeyGets201`, `.redisDownGives202`.
- [ ] **Step 2:** `./mvnw -f chat-service verify`. Expected: FAIL.
- [ ] **Step 3:** Hiện thực theo spec §4.4 (thứ tự: chống trùng → `reserveSeq` → ghi `messages`, `send_dedup`, Redis → sự kiện → commit). Metric: `chat.send.latency` (từ `received_at_ms` của lệnh tới lúc ghi sự kiện), `chat.sequencer.lag`, `chat.lwt.conflicts`, `chat.dedup.hits`.
- [ ] **Step 4:** Chạy lại. Expected: PASS.
- [ ] **Step 5:** Commit `feat(chat): sequencer with LWT fencing, dedup, events, HTTP send result`.

---

### Task 6: Fan-out qua Push API

**Files:**
- Create: `core/FanOutPlanner.java`, `core/Pusher.java` (interface); `grpc/PushApiClient.java`; `kafka/FanOutLoop.java`; `core/PushNotifier.java` (hiện thực `Notifier` của Task 4)
- Test: `core/FanOutPlannerTest`, it `grpc/PushApiClientIT`, it `kafka/FanOutIT`

**Interfaces:**
- Consumes: `ChatEvent` (Task 5), `MemberStore` + `RedisChatCache` (Task 3).
- Produces:
  - `FanOutPlanner.plan(ChatEvent e, List<Member> members): List<PushRequest>`: DIRECT/GROUP → một MESSAGE RELIABLE mỗi thành viên (`principal_id=chat:<userId>`, `headers.type=chat.message`, `payload=ChatMessage`, `tenant_id=chat`, không `idempotency_key`); CHANNEL → một BROADCAST EPHEMERAL `topic=chat:chan:<convId>`.
  - `interface Pusher { PushResponse send(PushRequest r) throws PushUnavailableException; }`; `PushApiClient implements Pusher` dùng một `SendStream`, ghép response theo `request_id`, timeout 2 s, mở lại stream khi đứt (như `gateway` `PushApiKickClient`).
  - `FanOutLoop`: group `chat-fanout`; mỗi sự kiện → `plan` → gửi; `UNAVAILABLE` / `RATE_LIMITED` → backoff 100 ms → 5 s, không commit; `INVALID` → metric và bỏ qua; DIRECT/GROUP → `touchUserConv` cho từng thành viên.
  - `PushNotifier.rejected(senderId, r)`: MESSAGE EPHEMERAL tới `chat:<senderId>`, `headers.type=chat.rejected`.

- [ ] **Step 1:** Test:
  - `FanOutPlannerTest.groupGetsOneReliableMessagePerMember` (3 thành viên → 3 request, principal đúng, RELIABLE, không idempotency key).
  - `FanOutPlannerTest.channelIsOneEphemeralBroadcast`.
  - `PushApiClientIT.answersMatchedByRequestId` (Push API thật trong JVM như `push-api` `SendStreamIT`).
  - `FanOutIT.unavailablePushApiIsRetriedWithoutLosingEvents` (Push API dừng rồi chạy lại → mọi sự kiện được gửi, offset chỉ commit sau khi gửi).
  - `FanOutIT.groupMembersInboxSeqIsUpdated`.
- [ ] **Step 2:** `./mvnw -f chat-service verify`. Expected: FAIL.
- [ ] **Step 3:** Hiện thực. Metric `chat.fanout.push{result}`, `chat.fanout.lag`. Lifecycle như `push-api`: HTTP và các loop dừng nhận trước (SIGTERM: xong lô đang chạy tối đa 10 s, commit), producer, Kafka, ScyllaDB, Redis đóng sau (`spring.lifecycle.timeout-per-shutdown-phase: 10s`).
- [ ] **Step 4:** Chạy lại. Expected: PASS.
- [ ] **Step 5:** Commit `feat(chat): fan-out to members and channels through Push API`.

---

### Task 7: API đọc và quản lý hội thoại

**Files:**
- Create: `core/SyncService.java`, `core/Conversations.java`; `http/QueryController.java`
- Test: `core/SyncTest`, `core/ConversationsTest`, it `http/QueryApiIT`

**Interfaces:**
- Consumes: Task 3 stores, Task 4 `AuthFilter` / `Caller`.
- Produces: các endpoint đúng spec §7. `SyncService.sync(String userId, List<SyncCursor> in, int perConv): List<SyncResult>` với `record SyncCursor(String convId, long lastSeq)`, `record SyncResult(String convId, List<StoredMessage> messages, boolean tooMany, long lastSeq)`; tối đa 100 hội thoại, `perConv` mặc định 50, tối đa 100. `Conversations.create(...)`, `openDm(userId, otherId)`, `addMember`, `removeMember` (xoá cache thành viên ngay), `join(userId, convId)` (chỉ kênh công khai).

- [ ] **Step 1:** Test:
  - `SyncTest.returnsMessagesAfterCursor`, `.tooManyWhenGapExceedsLimit`, `.clientAheadGetsNothing` (Review Focus 3: `lastSeq` > `last_seq` → rỗng, `tooMany=false`), `.notMemberIsSkipped`.
  - `ConversationsTest.groupOver500IsRejected`, `.dmWithSelfIsInvalid`, `.onlyOwnerOrAdminManagesMembers`, `.anyoneCanLeave`, `.joinPrivateChannelIsForbidden`.
  - `QueryApiIT.historyPagesByConvSeq`, `.conversationsSortedByLastMessage`, `.removedMemberStopsReceivingAtOnce` (Review Focus 2: xoá B khỏi nhóm rồi gửi tin → `FanOutPlanner` không tạo request cho B; không chờ hết TTL cache), `.problemJsonCodes` (`invalid_token`, `forbidden`, `not_found`).
- [ ] **Step 2:** `./mvnw -f chat-service verify`. Expected: FAIL.
- [ ] **Step 3:** Hiện thực. Metric `chat.api.requests{route, code}`.
- [ ] **Step 4:** Chạy lại. Expected: PASS.
- [ ] **Step 5:** Commit `feat(chat): conversations, history, sync and membership API`.

---

### Task 8: Mở và đóng kênh

**Files:**
- Create: `core/ChannelSubscriptions.java`
- Modify: `kafka/UpstreamCommandListener.java` (thêm `op=chat.channel.open` / `chat.channel.close`)
- Test: `core/ChannelSubscriptionsTest`

**Interfaces:**
- Consumes: `Pusher` (Task 6), `ConversationStore` / `MemberStore`.
- Produces: `ChannelSubscriptions.open(String userId, String convId): Optional<Rejection>` → `PushRequest` CONTROL `subscribe {chat:chan:<convId>}` tới `chat:<userId>`; `close` → `unsubscribe`. Quyền: kênh công khai, hoặc là thành viên kênh riêng tư; không phải kênh → `invalid`.

- [ ] **Step 1:** `ChannelSubscriptionsTest.publicChannelSubscribes`, `.privateChannelNeedsMembership` (`not_member`, không gửi CONTROL), `.groupIsNotAChannel`, `.closeUnsubscribes`.
- [ ] **Step 2:** `./mvnw -f chat-service test -Dtest=ChannelSubscriptionsTest`. Expected: FAIL.
- [ ] **Step 3:** Hiện thực.
- [ ] **Step 4:** Chạy lại. Expected: PASS.
- [ ] **Step 5:** Commit `feat(chat): channel open/close through server-side subscribe`.

---

### Task 9: End to end, devenv smoke, tài liệu, đo đạc

**Files:**
- Create: `chat-service/src/it/java/com/pangate/chat/it/ChatE2EIT.java`, `support/ChatHarness.java` (chat-service + Push API + router + 2 gateway trong một JVM, như `push-api` `PushApiIT`; JWKS qua `HttpServer` như `PushApiHarness`); `chat-service/README.md`; `docs/adr/0028-…` đến `0032-…` (spec §11); cập nhật `docs/adr/README.md`, `CHANGELOG.md`, `README.md`, trạng thái spec
- Modify: `gateway-devtools/.../SmokeCommand.java` (`--chat <url>`: bước `CHAT 1-1`, `CHAT group`, `CHAT channel`), `SmokeCommandTest`; `Makefile` (`smoke` truyền `--chat`)
- Test: `ChatE2EIT`, `SmokeCommandTest`

**Interfaces:**
- Consumes: mọi task trước; devtools `GatewayClient.upstream(type, msgId, payload, timeout)`, `awaitPush`, `ack`.

- [ ] **Step 1:** `ChatE2EIT`:
  - `directMessageArrivesInConvSeqOrder` (A gửi 3 tin qua WebSocket; B nhận `ChatMessage` `convSeq` 1, 2, 3; thiết bị thứ hai của A cũng nhận).
  - `groupOfThreeGetsEveryMessage`.
  - `channelOpenThenBroadcast` (B gửi `chat.channel.open`, owner đăng tin → B nhận; trước khi mở thì không).
  - `reconnectAndSyncGetsWhatWasMissed` (B ngắt, A gửi 5 tin, B nối lại, `POST /v1/sync` trả đủ 5).
  - `resendSameClientMsgIdIsOneMessage` (WebSocket gửi 2 lần + HTTP 1 lần cùng `clientMsgId` → một tin).
  - `rejectedSendIsPushedBackToSender` (không phải thành viên → `chat.rejected` `not_member`).
- [ ] **Step 2:** `./mvnw -f chat-service verify -Dit.test=ChatE2EIT`. Expected: FAIL trước khi có harness; sau khi có harness, mọi case phải đỏ đúng lý do nếu tắt FanOut.
- [ ] **Step 3:** Hiện thực harness, bước smoke, tài liệu, ADR 0028–0032 (nội dung đúng bảng spec §11).
- [ ] **Step 4:** `make verify`; `make release-check`; `make images up smoke down` (bước `CHAT 1-1`, `CHAT group`, `CHAT channel` OK). Đo: nhóm 500, kênh 10 000 phiên, ghi p99 gửi → nhận vào README và trạng thái spec. Expected: tất cả xanh; p99 ghi lại kèm cấu hình máy.
- [ ] **Step 5:** Commit `test(chat): end to end with Push API, router and gateways; smoke, docs, ADR 0028-0032`.
