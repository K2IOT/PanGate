# Trạng thái hội thoại CH3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thêm vào chat-service: mốc đã nhận / đã xem theo người, số chưa đọc đồng bộ giữa thiết bị, "đang gõ", online / lần cuối hoạt động theo danh sách client đăng ký xem, cài đặt riêng tư có qua có lại, và API `/v1/state` cho hợp đồng đồng bộ phía client.

**Architecture:** Mọi thứ trong `chat-service` (spec §1). Lệnh mốc đi `upstream.cmd → Ingest → chat.receipts → ReceiptWorker` (gộp theo hội thoại, ghi ScyllaDB `USING TIMESTAMP <mốc>`, đẩy EPHEMERAL). Typing đi thẳng `Ingest → TypingRelay → Push API`. Online: `PresenceTracker` đọc `c1.gw.lifecycle` vào Redis, `PresenceFanOut` đẩy cho các thiết bị đang watch. Lõi trong `core/` không import Spring, như CH1.

**Tech Stack:** Như CH1: Java 21, Spring Boot 3.5.16, ScyllaDB (driver DataStax 4.x), Lettuce, Kafka clients, gRPC tới Push API; Testcontainers (Scylla, Kafka, Redis).

**Spec:** `docs/superpowers/specs/2026-11-17-conversation-state-design.md`

## Global Constraints

- Không sửa gateway, router, auth, Push API, account-service (spec §1).
- Contracts chỉ thêm vào `platform-contracts/proto/pangate/v1/chat.proto`: `ChatMark`, `ChatTyping`, `PresenceWatch`, `ReceiptUpdate`, `TypingUpdate`, `PresenceUpdate`, `ReceiptCommand` đúng field number của spec §2; `buf breaking` xanh.
- Op upstream: `chat.delivered`, `chat.read`, `chat.typing`, `chat.presence.watch`. Header `type` của PUSH: `chat.receipts`, `chat.typing`, `chat.presence` (spec §2).
- Mọi push trạng thái là `class=EPHEMERAL`; typing `ttl_sec=10`, presence `ttl_sec=30`, receipts `ttl_sec=30` (spec §1, §5, §6.3).
- Topic `chat.receipts`: 64 partition, retention 1 ngày, key `conversation_id` (spec §9).
- Cột mốc (`delivered_seq`, `read_seq` ở `members` và `user_convs`) chỉ được ghi bằng `USING TIMESTAMP <giá trị mốc>`; không chỗ nào khác ghi chúng (spec §3.2).
- Bảng `members` ≤ 50 người có danh sách đã xem; 51–500 chỉ `readCount`; kênh không có receipts (spec S3, §3.4).
- Cửa sổ gộp: DM 500 ms, nhóm 2 s; kênh ghi ngay, không đẩy (spec §3.4).
- Giới hạn mốc: 50/giây/user, burst 100; typing 1 lệnh / 2 s / (user, hội thoại); typing fan-out ≤ 1 lượt/giây/hội thoại/pod; `GET .../receipts` 1/giây/user/hội thoại (spec §3.3, §3.4, §5).
- `unread = max(0, lastSeq − readSeq)`, trả về tối đa 999 (spec §3.2).
- Online: ân hạn offline 5 s; `last_seen_ms` làm tròn xuống phút; quá 7 ngày → `recently=true`, `last_seen_ms=0`; tập `pres:*` TTL 24 h gia hạn mỗi lần ghi (spec §6.1, §6.2).
- Watch: tối đa 200 mục, TTL 5 phút, mỗi lần gửi thay toàn bộ danh sách của thiết bị; mục sai bỏ lặng lẽ; kênh không được (spec §6.3).
- Riêng tư đúng bảng spec §8; cache `user_settings` 60 s; `delivered_seq` không bị `hideRead` ảnh hưởng.
- `ttl_ms` của `TypingUpdate` = 6000 (spec §2, §5).
- Lỗi HTTP: `application/problem+json`, code ổn định như CH1 (`invalid_request`, `not_found`, `forbidden`, `rate_limited`, `unavailable`, `invalid_token`) và `not_supported` cho receipts của kênh.
- Metrics đúng tên spec §9; task nào sở hữu khối nào thì đếm metric của khối đó (`chat.receipts.*` Task 3–4, `chat.typing.*` Task 6, `chat.presence.transitions` Task 7, `chat.presence.watchers/pushes/unavailable` Task 8, `chat.state.requests` Task 5).
- Redis lỗi ở đường online: không ném ra ngoài listener; đếm `chat.presence.unavailable`, coi user là offline, watch không lưu; tự phục hồi khi Redis về (spec §9).
- Mỗi commit kết thúc bằng hai dòng trailer của session (Co-Authored-By, Claude-Session).

## Review Focus

1. **Mốc vượt `last_seq`** (client tính sai, hoặc gửi mốc trước khi tin tới cache 5 s): server kẹp về `last_seq`, không bao giờ để `unread` âm hay mốc đứng trước tin chưa có. Test: `MarkValidatorTest.markAboveLastSeqIsClamped` (Task 3).
2. **Người rời nhóm vẫn được thấy online qua watch cũ**: sau khi rời, lần watch kế tiếp (≤ 5 phút) phải bị từ chối mục đó; người đã rời không nhận receipts/typing của nhóm. Test: `PresenceWatchTest.leftMemberIsNotWatchedAfterRefresh` (Task 8), `ReceiptRecipientsTest.leftMemberGetsNothing` (Task 4).
3. **`hideRead` lộ gián tiếp** qua `readCount`, `peer` trong `/v1/conversations` hay `/v1/state`. Test: `StateApiIT.hideReadNeverLeaksThroughPeerOrCount` (Task 5).
4. **Hai thiết bị, một ngắt**: còn thiết bị kia thì vẫn online, không phát chuyển trạng thái. Test: `PresenceTrackerTest.secondDeviceKeepsOnline` (Task 7).
5. **Nối lại sang node khác trong ân hạn, rồi node cũ `NODE_DOWN`**: vẫn online, không phát offline. Test: `PresenceTrackerTest.nodeDownOfOldNodeAfterReconnectStaysOnline` (Task 7).

---

### Task 1: Contracts và testkit

**Files:**
- Modify: `platform-contracts/proto/pangate/v1/chat.proto`
- Modify: `platform-contracts-testkit/src/main/java/com/pangate/contracts/testkit/ChatTopics.java`
- Create: `platform-contracts-testkit/src/main/java/com/pangate/contracts/testkit/ChatStateCases.java`
- Test: `platform-contracts-testkit/src/test/java/com/pangate/contracts/testkit/ChatStateCasesTest.java`
- Modify: `platform-devenv/docker-compose.yml` (kafka-init: `chat.receipts` 64 partition, `retention.ms=86400000`)

**Interfaces:**
- Produces: các message của spec §2 (Java `com.pangate.contracts.v1.*`); `ChatTopics.RECEIPTS = "chat.receipts"`; `ChatTopics.OP_DELIVERED/OP_READ/OP_TYPING/OP_WATCH` (`"chat.delivered"`, `"chat.read"`, `"chat.typing"`, `"chat.presence.watch"`); `ChatTopics.TYPE_RECEIPTS/TYPE_TYPING/TYPE_PRESENCE`; `ChatStateCases.validMarks()`, `invalidMarks()` (mỗi case `record MarkCase(String name, ChatMark mark, String reason)`), `validWatches()`, `invalidWatches()` (`record WatchCase(String name, PresenceWatch watch, String reason)`).

- [ ] **Step 1: Write the failing test** `ChatStateCasesTest`:
  - `invalidMarksCoverEveryRule`: các reason của `invalidMarks()` ⊇ `{"invalid"}` với case conv không phải ULID và case `up_to_seq = 0`.
  - `invalidWatchesCoverEveryRule`: có case 201 mục (`invalid`), mục `user_id` rỗng, mục `conversation_id` không phải ULID.
  - `receiptsTopicNameIsPinned`: `assertThat(ChatTopics.RECEIPTS).isEqualTo("chat.receipts")`.
- [ ] **Step 2:** `./mvnw -q -f platform-contracts-testkit test -Dtest=ChatStateCasesTest` → FAIL (compile: không có `ChatStateCases`).
- [ ] **Step 3:** Thêm message vào `chat.proto` đúng spec §2; thêm hằng và `ChatStateCases`; thêm topic vào kafka-init.
- [ ] **Step 4:** `./mvnw -q -f platform-contracts install && ./mvnw -q -f platform-contracts-testkit install` → PASS; `make contracts-lint` (buf lint + breaking) xanh.
- [ ] **Step 5: Commit** `feat(contracts): chat receipts, typing and presence messages; chat.receipts topic`

---

### Task 2: Schema V3 và store mốc / cài đặt / lần cuối hoạt động

**Files:**
- Create: `chat-service/src/main/resources/cql/V3__state.cql`
- Modify: `chat-service/src/main/java/com/pangate/chat/core/ChatStores.java`
- Modify: `chat-service/src/main/java/com/pangate/chat/store/ScyllaChatStore.java`
- Modify: `chat-service/src/main/java/com/pangate/chat/kafka/FanOutLoop.java` (mốc của người gửi)
- Modify: `chat-service/src/test/java/com/pangate/chat/core/FakeStores.java`
- Test: `chat-service/src/it/java/com/pangate/chat/store/ChatStoreIT.java`, `SchemaMigratorIT.java`, `chat-service/src/test/java/com/pangate/chat/kafka/FanOutLoopTest.java`

**Interfaces:**
- Produces:
  - `V3__state.cql`: `ALTER TABLE members ADD delivered_seq bigint, read_seq bigint`; `ALTER TABLE user_convs ADD delivered_seq bigint, state_at bigint`; `CREATE TABLE user_settings (user_id text PRIMARY KEY, hide_presence boolean, hide_read boolean, updated_at bigint)`; `CREATE TABLE last_seen (user_id text PRIMARY KEY, last_seen_ms bigint)`. Mỗi lệnh idempotent (`ALTER` bỏ qua lỗi "already exists" như cách `SchemaMigrator` đang làm, hoặc kiểm `system_schema.columns` trước).
  - `record Marks(long deliveredSeq, long readSeq)`; `record MemberMarks(String userId, long deliveredSeq, long readSeq)`.
  - `interface ChatStores.MarkStore { void advance(String convId, String userId, long deliveredSeq, long readSeq, long atMs); void touchState(String userId, String convId, long atMs); List<MemberMarks> marks(String convId); Marks own(String userId, String convId); List<UserConvState> changedSince(String userId, long sinceMs, String cursor, int limit); }` — `advance` ghi `members` và `user_convs` với `USING TIMESTAMP` = giá trị mốc cho từng cột (0 = không ghi cột đó), và `state_at`; `touchState` ghi `state_at` của dòng `user_convs` (cho người kia trong DM).
  - `record UserConvState(String convId, ConversationKind kind, long lastSeq, long readSeq, long deliveredSeq, long stateAtMs)`.
  - `UserConv` thêm `long deliveredSeq` (record CH1 mở rộng; mọi chỗ tạo `UserConv` cập nhật).
  - `interface ChatStores.SettingsStore { Privacy privacy(String userId); void putPrivacy(String userId, Privacy p, long atMs); }`, `record Privacy(boolean hidePresence, boolean hideRead) { static final Privacy OPEN = new Privacy(false, false); }`.
  - `interface ChatStores.LastSeenStore { OptionalLong lastSeen(String userId); void putLastSeen(String userId, long atMs); }`.
  - `touchUserConv` (CH1) ghi thêm `state_at`.
  - `FanOutLoop`: với mỗi tin, gọi `marks.advance(conv, senderId, convSeq, convSeq, sentAtMs)` một lần (mọi loại hội thoại, kể cả kênh) — spec §3.1.

- [ ] **Step 1: Write the failing tests:**
  - `ChatStoreIT.marksNeverGoBack`: `advance(c,u,5,3)`, rồi `advance(c,u,4,2)` → `own(u,c) == Marks(5,3)` và `marks(c)` có `MemberMarks(u,5,3)`; rồi `advance(c,u,0,6)` → `readSeq=6`, `deliveredSeq=5` (store giữ từng cột độc lập; bất biến `delivered ≥ read` do `ReceiptWorker` bảo đảm khi truyền `delivered = max(delivered, read)`, Task 4).
  - `ChatStoreIT.changedSinceReturnsOnlyNewer`: hai hội thoại, `touchState` một cái ở t=200; `changedSince(u,150,null,10)` chỉ trả hội thoại đó.
  - `ChatStoreIT.privacyAndLastSeenRoundTrip`: không có dòng → `Privacy.OPEN`, `OptionalLong.empty()`; ghi rồi đọc lại.
  - `SchemaMigratorIT.v3RunsTwice`: chạy migrator hai lần không lỗi, cột mới tồn tại.
  - `FanOutLoopTest.senderMarksAdvanceWithEachMessage`: sau tin seq 7 của `a` → `FakeStores.own("a",conv) == Marks(7,7)`; với kênh cũng vậy.
- [ ] **Step 2:** `./mvnw -q -f chat-service verify -Dit.test='ChatStoreIT,SchemaMigratorIT' -Dtest=FanOutLoopTest` → FAIL (compile).
- [ ] **Step 3:** Hiện thực như Interfaces; `FakeStores` giữ mốc bằng `Math::max` theo từng cột (mô phỏng `USING TIMESTAMP`).
- [ ] **Step 4:** Lệnh ở Step 2 → PASS; `./mvnw -q -f chat-service test` xanh.
- [ ] **Step 5: Commit** `feat(chat): V3 schema, monotonic receipt marks, privacy and last-seen stores; sender marks on fan-out`

---

### Task 3: Ingest mốc (WebSocket + HTTP) vào `chat.receipts`

**Files:**
- Create: `chat-service/src/main/java/com/pangate/chat/core/MarkValidator.java`, `core/MarkSink.java`, `kafka/ReceiptProducer.java`
- Modify: `kafka/UpstreamCommandListener.java` (op `chat.delivered`, `chat.read`), `http/StateController.java` (mới: `POST /v1/conversations/{id}/read`, `.../delivered`), `config/ChatConfig.java`, `config/ChatProperties.java`
- Test: `chat-service/src/test/java/com/pangate/chat/core/MarkValidatorTest.java`, `src/it/java/com/pangate/chat/kafka/UpstreamCommandListenerIT.java`

**Interfaces:**
- Consumes: `MemberLookup` (CH1), `ConversationStore.lastSeq`, `TokenBucketLimiter` (CH1), `ChatStateCases` (Task 1).
- Produces:
  - `MarkValidator(MemberLookup members, LongCache lastSeqCache, TokenBucketLimiter perUser)` với `Optional<ReceiptCommand> check(String userId, String deviceId, ChatMark m, boolean read, long nowMs)`; trả rỗng khi bỏ (đếm `chat.receipts.rejected{reason}` qua callback `Consumer<String> onReject`); kẹp `up_to_seq` về `lastSeq`. `read=true` đặt `read_seq = delivered_seq = up_to_seq`; `read=false` chỉ `delivered_seq`. `lastSeqCache` TTL 5 s.
  - `interface MarkSink { void publish(ReceiptCommand c); }` — `ReceiptProducer` ghi `chat.receipts` key `conversation_id`, `acks=all`; lỗi Kafka → `QueueUnavailableException` (CH1) để HTTP trả `503`.
  - `ChatProperties.State` (mới, prefix `chat.state`): `markRate=50`, `markBurst=100`, `dmWindow=500ms`, `groupWindow=2s`, `listedReceiptsMax=50`, `offlineGrace=5s`, `watchTtl=5m`, `watchMax=200`, `typingEvery=2s`.

- [ ] **Step 1: Write the failing tests** (`MarkValidatorTest`, chạy cả case testkit):
  - `testkitCasesAreRefused`: mỗi `ChatStateCases.invalidMarks()` → rỗng với đúng reason.
  - `nonMemberIsDropped`: reason `not_member`.
  - `markAboveLastSeqIsClamped` (Review Focus 1): `lastSeq=10`, mark 15 → `read_seq=10`.
  - `readImpliesDelivered`: `read=true`, 7 → `delivered_seq=7, read_seq=7`.
  - `rateLimitedAfterBurst`: 101 lệnh liên tiếp → lệnh 101 rỗng, reason `rate_limited`.
  - `publicChannelFollowerMayMark`: thành viên kênh → được.
  - `UpstreamCommandListenerIT.readOpLandsOnReceiptsTopic`: UPSTREAM `op=chat.read` → một record `chat.receipts` key `conv`, payload `ReceiptCommand{read_seq}`; payload hỏng → `chat.commands.dlq`.
- [ ] **Step 2:** `./mvnw -q -f chat-service verify -Dtest=MarkValidatorTest -Dit.test=UpstreamCommandListenerIT` → FAIL.
- [ ] **Step 3:** Hiện thực; `StateController` trả `204`, lỗi `invalid_request` (400) cho body sai, `forbidden` (403) cho `not_member`, `rate_limited` (429), `unavailable` (503).
- [ ] **Step 4:** Lệnh Step 2 → PASS.
- [ ] **Step 5: Commit** `feat(chat): accept delivered/read marks over WebSocket and HTTP into chat.receipts`

---

### Task 4: `ReceiptWorker` và lọc riêng tư

**Files:**
- Create: `core/PrivacyRules.java`, `core/PrivacyCache.java`, `core/ReceiptAggregator.java`, `core/ReceiptRecipients.java`, `core/StatePushes.java`, `kafka/ReceiptWorkerLoop.java`
- Modify: `config/ChatConfig.java`
- Test: `src/test/java/com/pangate/chat/core/PrivacyRulesTest.java`, `ReceiptAggregatorTest.java`, `ReceiptRecipientsTest.java`, `src/it/java/com/pangate/chat/kafka/ReceiptWorkerIT.java`

**Interfaces:**
- Consumes: `MarkStore`, `SettingsStore`, `Privacy` (Task 2); `ReceiptCommand` (Task 1); `MemberLookup`, `Pusher` (CH1).
- Produces:
  - `PrivacyCache(SettingsStore s, LongSupplier monoMs)`: `Privacy of(String userId)` cache 60 s; `void forget(String userId)`.
  - `PrivacyRules`: `static boolean mayShowPresence(Privacy subject, Privacy viewer)` = `!subject.hidePresence() && !viewer.hidePresence()`; `static boolean mayShowRead(Privacy subject, Privacy viewer)` = `!subject.hideRead() && !viewer.hideRead()`.
  - `ReceiptAggregator(LongSupplier monoMs, Duration dmWindow, Duration groupWindow)`: `void add(ReceiptCommand c, ConversationKind kind)`; `List<Flush> due()`; `record Flush(String convId, ConversationKind kind, Map<String, Marks> byUser, long lastOffsetHint)`. Giữ max theo từng cột; kênh `due` ngay.
  - `ReceiptRecipients`: `static List<PushRequest> plan(Flush f, List<Member> members, Function<String, Privacy> privacy, int listedMax)` — đồng bộ thiết bị chính người đọc (`own_read_seq`, tới `chat:<reader>`) khi `read` tăng; cho người khác: DM và nhóm ≤ `listedMax` một `ReceiptUpdate{marks}` mỗi người nhận, `read_seq` bị thay bằng 0 cho cặp mà `mayShowRead` sai (giữ `delivered_seq`); nhóm > `listedMax` và kênh: không ai.
  - `StatePushes`: `static PushRequest ephemeral(String userId, String deviceIdOrEmpty, String type, ByteString payload, int ttlSec)`.
  - `ReceiptWorkerLoop` (group `chat-receipts`): poll → aggregator → mỗi `Flush`: `marks.advance(...)` cho từng người (với `delivered = max(delivered, read)`), `touchState` dòng của người kia nếu DM, rồi đẩy (3 lần thử, lỗi thì bỏ, đếm `chat.receipts.pushes{result}`); commit offset tới record cuối đã ghi ScyllaDB; ScyllaDB lỗi → không commit, `seek` lại, backoff 100 ms → 5 s.

- [ ] **Step 1: Write the failing tests:**
  - `PrivacyRulesTest`: đủ bốn ô của mỗi luật (spec §8), tên `presenceIsReciprocal`, `readIsReciprocal`.
  - `ReceiptAggregatorTest.keepsTheMaxOutOfOrder` (mốc 5, 3, 4 → 5), `dmFlushesAfter500ms`, `groupFlushesAfter2s`, `channelFlushesAtOnce`.
  - `ReceiptRecipientsTest`: `dmPushesPeerAndOwnDevices`; `group50ListsMarks`; `group51PushesNobodyButOwnDevices`; `hideReadZeroesReadButKeepsDelivered`; `leftMemberGetsNothing` (Review Focus 2: người không còn trong `members` không có trong danh sách nhận).
  - `ReceiptWorkerIT.outOfOrderAndReplayNeverRegress`: gửi mốc 9 rồi 4 rồi replay 9 → ScyllaDB `Marks(9,9)`; `ReceiptWorkerIT.dmPeerGetsReceiptWithinASecond` (Push API giả của CH1 IT ghi lại request).
- [ ] **Step 2:** `./mvnw -q -f chat-service verify -Dtest='PrivacyRulesTest,ReceiptAggregatorTest,ReceiptRecipientsTest' -Dit.test=ReceiptWorkerIT` → FAIL.
- [ ] **Step 3:** Hiện thực như Interfaces.
- [ ] **Step 4:** Lệnh Step 2 → PASS.
- [ ] **Step 5: Commit** `feat(chat): receipt worker — windowed per-conversation marks, own-device sync, reciprocal privacy`

---

### Task 5: API trạng thái: chưa đọc, `/v1/state`, receipts, riêng tư

**Files:**
- Modify: `http/QueryController.java` (`GET /v1/conversations` thêm `readSeq`, `deliveredSeq`, `unread`, `peer` cho DM), `http/StateController.java`
- Create: `core/StateService.java`
- Test: `src/test/java/com/pangate/chat/core/StateServiceTest.java`, `src/it/java/com/pangate/chat/http/StateApiIT.java`

**Interfaces:**
- Consumes: `MarkStore.changedSince/own/marks`, `PrivacyCache`, `PrivacyRules` (Task 2, 4); `ConversationStore.lastSeq`.
- Produces:
  - `StateService`: `static int unread(long lastSeq, long readSeq)` (`max(0, …)`, chặn 999); `Map<String,Object> conversationView(String viewer, UserConvState s, Optional<MemberMarks> peerMarks)`; `StatePage state(String viewer, OptionalLong sinceMs, String cursor)` (`record StatePage(List<Map<String,Object>> conversations, String cursor, long now)`, tối đa 200); `ReceiptsView receipts(String viewer, String convId, long seq)` — DM / ≤ 50: `marks` đã lọc; 51–500: `readCount` (bỏ người gửi tin `seq`, bỏ người `hideRead`; viewer `hideRead` thì trả `{readCount: null}` vì có qua có lại), cache 5 s theo `(conv, seq)`; kênh → `ChatRefusal("not_supported")`.
  - Endpoint: `GET /v1/state?since=&cursor=`; `GET /v1/conversations/{id}/receipts?seq=` (giới hạn 1/giây/user/hội thoại → 429); `GET /v1/me/privacy` → `{hidePresence, hideRead}`; `PUT /v1/me/privacy` body `{hidePresence?, hideRead?}` → `204` và `PrivacyCache.forget`.
  - Kênh: `unread` dùng `conversations.last_seq` (spec §3.2).

- [ ] **Step 1: Write the failing tests:**
  - `StateServiceTest.unreadIsClampedTo0And999` (`unread(5,9)==0`, `unread(2000,0)==999`).
  - `StateServiceTest.channelUnreadUsesConversationLastSeq`.
  - `StateApiIT.listCarriesUnreadAndPeer`: A gửi 3 tin tới B; B đọc tới 2 → `GET /v1/conversations` của B: `unread=1`; của A: `peer.readSeq=2`.
  - `StateApiIT.stateSinceReturnsChangedConversations`: chỉ hội thoại có thay đổi sau `since`; `now` có trong body.
  - `StateApiIT.receiptsOfLargeGroupIsACount`: nhóm 60, 3 người đọc tới N → `readCount=3`; kênh → 404 `not_supported`.
  - `StateApiIT.hideReadNeverLeaksThroughPeerOrCount` (Review Focus 3): B `hideRead` → A thấy `peer.readSeq=0` ở `/v1/conversations` và `/v1/state`, B không có trong `marks`, không tính vào `readCount`; A `hideRead` → A không thấy mốc đọc của ai.
- [ ] **Step 2:** `./mvnw -q -f chat-service verify -Dtest=StateServiceTest -Dit.test=StateApiIT` → FAIL.
- [ ] **Step 3:** Hiện thực.
- [ ] **Step 4:** Lệnh Step 2 → PASS; `QueryApiIT` cũ vẫn xanh (field mới chỉ thêm).
- [ ] **Step 5: Commit** `feat(chat): unread counts, /v1/state, receipts lookup and privacy settings API`

---

### Task 6: Đang gõ

**Files:**
- Create: `core/TypingRelay.java`
- Modify: `kafka/UpstreamCommandListener.java` (op `chat.typing`), `config/ChatConfig.java`
- Test: `src/test/java/com/pangate/chat/core/TypingRelayTest.java`

**Interfaces:**
- Consumes: `MemberLookup`, `Pusher`, `StatePushes.ephemeral` (Task 4); `OpenConversations` (Task 8 — trong task này dùng interface, Task 8 cung cấp bản thật).
- Produces:
  - `interface OpenConversations { Set<String> devicesWithOpen(String convId); }` trả `"<userId>/<deviceId>"`; bản rỗng `OpenConversations.NONE` dùng tới Task 8.
  - `TypingRelay(MemberLookup members, Pusher pusher, OpenConversations open, LongSupplier monoMs, int listedMax)`: `void typing(String userId, String convId)` — bỏ im lặng khi: không phải thành viên, hội thoại là kênh, `(user, conv)` đã có lệnh trong 2 s, hội thoại đã đẩy trong 1 s qua. Nhóm ≤ `listedMax`: đẩy `TypingUpdate{ttl_ms=6000}` tới mọi thành viên khác (principal, không đặt thiết bị); nhóm lớn hơn: chỉ tới thiết bị trong `open.devicesWithOpen(conv)` (đặt `device_id`). Đếm `chat.typing.relayed`, `chat.typing.dropped{reason}`.

- [ ] **Step 1: Write the failing tests** (`TypingRelayTest`, Pusher ghi lại): `dmTypingReachesPeerOnly`; `secondTypingWithin2sIsDropped`; `channelTypingIsDropped`; `nonMemberIsDropped`; `largeGroupGoesOnlyToDevicesWithItOpen` (nhóm 60, `open` trả 2 thiết bị → đúng 2 request có `device_id`); `perConversationFanOutAtMostOncePerSecond`; `ttlIs6000AndClassEphemeral`.
- [ ] **Step 2:** `./mvnw -q -f chat-service test -Dtest=TypingRelayTest` → FAIL.
- [ ] **Step 3:** Hiện thực; listener gọi `typing` trực tiếp (không ghi Kafka).
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(chat): typing indicators relayed directly as ephemeral pushes`

---

### Task 7: Theo dõi online từ `c1.gw.lifecycle`

**Files:**
- Create: `core/PresenceTracker.java`, `core/PresenceState.java` (port), `store/RedisPresence.java`, `kafka/LifecycleListener.java`
- Modify: `config/ChatConfig.java`
- Test: `src/test/java/com/pangate/chat/core/PresenceTrackerTest.java`, `src/it/java/com/pangate/chat/store/RedisPresenceIT.java`, `src/it/java/com/pangate/chat/kafka/LifecycleListenerIT.java`

**Interfaces:**
- Consumes: `ConnectionEvent` (contracts có sẵn), `LastSeenStore` (Task 2).
- Produces:
  - `interface PresenceState { boolean add(String userId, String node, long epoch, String connId); boolean remove(String userId, String node, long epoch, String connId); List<String> dropNode(String node, long epoch); boolean online(String userId); boolean seen(String eventId); void markGone(String userId, long atMs); OptionalLong gone(String userId); void clearGone(String userId); OptionalLong lastSeen(String userId); void putLastSeen(String userId, long atMs); }` — `add`/`remove` trả `true` khi tập đổi giữa rỗng và không rỗng. Redis: `pres:{user}`, `presnode:{node}:{epoch}`, `presgone:{user}`, `lastseen:{user}`, `presev:{eventId}` (TTL 1 h); TTL 24 h gia hạn mỗi lần ghi.
  - `PresenceTracker(PresenceState s, LastSeenStore durable, Consumer<Transition> onTransition, LongSupplier wallMs, Duration grace, ScheduledExecutorService timer)`: `void on(ConnectionEvent e)`; `record Transition(String userId, boolean online, long atMs)`. Rỗng → hẹn kiểm sau `grace`; khi tới hạn mà vẫn rỗng và `gone` còn → `putLastSeen` (Redis + ScyllaDB) và phát offline. Không rỗng sau khi đã offline → phát online (không phát nếu đang trong ân hạn).
  - `LifecycleListener` (group `chat-presence`, topic `PushTopics.lifecycle("c1")`): chỉ principal tiền tố `chat:`; record hỏng → đếm, bỏ.

- [ ] **Step 1: Write the failing tests** (`PresenceTrackerTest`, `PresenceState` giả trong bộ nhớ, timer ảo):
  - `connectMakesOnline`; `disconnectGoesOfflineAfterGrace` (4,9 s chưa phát; 5 s phát offline, `last_seen` ghi);
  - `reconnectWithinGraceEmitsNothing`;
  - `secondDeviceKeepsOnline` (Review Focus 4);
  - `nodeDownDropsItsConnections`;
  - `nodeDownOfOldNodeAfterReconnectStaysOnline` (Review Focus 5: CONNECTED n1, CONNECTED n2, NODE_DOWN n1 → vẫn online, không phát);
  - `duplicateEventIdIgnored`; `nonChatPrincipalIgnored`.
  - `RedisPresenceIT.setsExpireAfter24h` (kiểm `PTTL` ≤ 24 h, > 0) và `dropNodeRemovesOnlyThatNode`.
  - `LifecycleListenerIT.connectedEventMakesUserOnline` (Kafka thật, Redis thật).
- [ ] **Step 2:** `./mvnw -q -f chat-service verify -Dtest=PresenceTrackerTest -Dit.test='RedisPresenceIT,LifecycleListenerIT'` → FAIL.
- [ ] **Step 3:** Hiện thực.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(chat): presence tracking from gateway lifecycle with a 5 s offline grace`

---

### Task 8: Watch online, đẩy thay đổi, `/v1/presence`

**Files:**
- Create: `core/PresenceWatch.java`, `core/PresenceView.java`, `core/PresenceFanOut.java`, `store/RedisWatchers.java`
- Modify: `kafka/UpstreamCommandListener.java` (op `chat.presence.watch`), `http/StateController.java` (`GET /v1/presence`), `config/ChatConfig.java` (nối `PresenceTracker.onTransition` → `PresenceFanOut`, `OpenConversations` thật → `TypingRelay`)
- Test: `src/test/java/com/pangate/chat/core/PresenceWatchTest.java`, `PresenceViewTest.java`, `src/it/java/com/pangate/chat/store/RedisWatchersIT.java`

**Interfaces:**
- Consumes: `PresenceState` (Task 7), `PrivacyCache`, `PrivacyRules`, `StatePushes` (Task 4), `MemberLookup`, `ConversationStore`.
- Produces:
  - `interface Watchers extends OpenConversations { void replace(String viewer, String device, List<PresenceWatch.Entry> valid, long expiresAtMs); List<String> watchersOf(String userId, long nowMs); }` — Redis `watchers:{user}` (sorted set, phần tử `<viewer>/<device>/<conv>`, điểm = hạn), `watching:{viewer}/{device}`; `devicesWithOpen(conv)` = các thiết bị có ít nhất một mục với `conv` còn hạn (Redis `watchconv:{conv}` sorted set cùng hạn).
  - `PresenceView`: `static PresenceUpdate.State state(String userId, boolean online, OptionalLong lastSeenMs, Privacy subject, Privacy viewer, long nowMs)` — làm tròn phút, > 7 ngày → `recently`, ẩn → `hidden=true` (các field khác mặc định).
  - `PresenceWatch`: `void watch(String viewer, String device, PresenceWatch msg)` — kiểm từng mục (cả hai là thành viên của `conversation_id`, không phải kênh, `user_id ≠ viewer`), bỏ mục sai lặng lẽ, > 200 mục thì lấy 200 đầu; `replace`; đẩy ảnh chụp `PresenceUpdate` tới đúng `device`.
  - `PresenceFanOut`: `void on(PresenceTracker.Transition t)` — `watchersOf(t.userId)`, lọc riêng tư, đẩy một `PresenceUpdate` tới từng thiết bị (`ttl_sec=30`).
  - `GET /v1/presence?ids=a,b&conversationId=X` (≤ 100 id) → `{states:[…]}`; id không chung X bị bỏ.

- [ ] **Step 1: Write the failing tests:**
  - `PresenceViewTest`: `roundsDownToMinute`; `olderThan7DaysIsRecently`; `hiddenWhenEitherSideHides`; `onlineHasNoLastSeen`.
  - `PresenceWatchTest`: `strangerEntryIsDroppedSilently` (không chung X → không có trong ảnh chụp, không lưu); `channelEntryIsDropped`; `snapshotGoesToTheWatchingDeviceOnly`; `secondWatchReplacesTheFirst`; `moreThan200KeepsFirst200`; `leftMemberIsNotWatchedAfterRefresh` (Review Focus 2); `hidePresenceViewerStillStoresWatchForTyping` (spec §6.3).
  - `RedisWatchersIT`: `expiredWatchersFallOff` (hạn 1 s, đợi 1,5 s → `watchersOf` rỗng); `devicesWithOpenFollowsEntries`.
- [ ] **Step 2:** `./mvnw -q -f chat-service verify -Dtest='PresenceViewTest,PresenceWatchTest' -Dit.test=RedisWatchersIT` → FAIL.
- [ ] **Step 3:** Hiện thực; nối dây trong `ChatConfig`.
- [ ] **Step 4:** PASS; `./mvnw -q -f chat-service test` xanh (TypingRelayTest vẫn xanh với `OpenConversations` thật bị thay bằng giả trong test).
- [ ] **Step 5: Commit** `feat(chat): presence watch with shared-conversation check, change fan-out and /v1/presence`

---

### Task 9: E2E, smoke, tài liệu

**Files:**
- Modify: `chat-service/src/it/java/com/pangate/chat/it/ChatE2EIT.java` (hoặc tạo `ChatStateE2EIT.java` cùng harness `ChatStack`)
- Modify: `gateway-devtools/src/main/java/com/pangate/devtools/cmd/ChatSmoke.java` (bước `CHAT receipts`, `CHAT typing`, `CHAT presence`), `gateway-devtools/src/main/java/com/pangate/devtools/client/*` nếu cần gửi op mới
- Create: `docs/adr/0037-*.md`, `0038-*.md`, `0039-*.md` (theo spec §13), cập nhật `docs/adr/README.md`
- Modify: `CHANGELOG.md`, `chat-service/README.md`, spec CH3 (trạng thái → đã hiện thực)
- Test: E2E IT và `make images up smoke down`

**Interfaces:**
- Consumes: mọi thứ ở Task 1–8; `ChatStack`, `KnownUsers` (CH1/CH2 IT).

- [ ] **Step 1: Write the failing E2E tests** (`ChatStateE2EIT`, 2 gateway thật, client devtools), mỗi tiêu chí thành công của spec §0 một test:
  - `dmDeliveredThenReadReachSenderWithinASecond`;
  - `group3ListsReaders`; `group60ReceiptsIsACount`;
  - `readOnDevice1ClearsUnreadOnDevice2` (online: `own_read_seq` trong 1 s; nối lại: `/v1/state`);
  - `typingReachesPeerAndExpires` (người ngoài không nhận);
  - `disconnectShowsOfflineAfterGraceToWatcher`; `flapUnder5sShowsNothing`; `strangerCannotWatch`;
  - `privacyIsReciprocal` (`hideRead`, `hidePresence`);
  - `gapIsFilledBySyncWithoutDuplicates` (spec §7: B bỏ 3 PUSH giữa chừng, thấy lỗ, `/v1/sync` bù, không trùng `msg_id`).
- [ ] **Step 2:** `./mvnw -q -f chat-service verify -Dit.test=ChatStateE2EIT` → FAIL (compile: client devtools chưa gửi được op mới).
- [ ] **Step 3:** Thêm vào `GatewayClient` của devtools: `mark(String conv, long seq, boolean read)`, `typing(String conv)`, `watch(List<PresenceWatch.Entry>)`, và chờ PUSH theo `headers.type`; sửa chỗ nối dây còn thiếu nếu test lộ ra; thêm ba bước smoke vào `ChatSmoke.STEPS`; viết ADR, CHANGELOG, README.
- [ ] **Step 4:** `./mvnw -q -f chat-service install` xanh toàn bộ (unit + IT); `./mvnw -q -f gateway-devtools install` xanh; `make images up smoke down` → `SMOKE OK (… chat receipts, chat typing, chat presence …)`.
- [ ] **Step 5: Commit** `feat(chat,devtools): CH3 end-to-end tests and smoke steps; docs: ADR 0037-0039`
- [ ] **Step 6 (đo đạc, ghi kết quả vào README):** nhóm 50 người cùng đọc (push/giây); 10 000 user watch 50 người, 1% chập chờn mỗi phút (push presence/giây, p99 trễ). Dùng `ChatLoadIT` làm khuôn.
