# Đợt 3a: tra cứu nóng — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ở quy mô 1 triệu người dùng / kênh 100 000 thành viên, mọi lệnh của một người trong một hội thoại tốn số lần đọc store không phụ thuộc số thành viên, mọi giới hạn tần suất kiểm trước mọi lần tra store, và badge của một thông báo tốn O(1) lệnh Redis.

**Architecture:** Kiểm "có phải thành viên" bằng một lần đọc dòng `members` (ScyllaDB, không cache); danh sách có cache chỉ còn cho DM/nhóm. Giới hạn theo người dùng (`TokenBucketLimiter`, mỗi pod) đặt trước bước tra trong `Ingest` / `MarkValidator` / `TypingRelay` và trên mọi API HTTP đọc/quản lý. Badge là hash Redis `badge:{user}` cập nhật idempotent kiểu "lấy max" bằng script Lua, dựng lại (gộp) từ `user_convs` khi thiếu hoặc quá 24 giờ.

**Tech Stack:** Java 21, Spring Boot 3.5, Lettuce (sync + async pipeline, `EVAL`), ScyllaDB, JUnit 5 + AssertJ, Testcontainers (`redis:7.4.2`).

**Spec:** `docs/superpowers/specs/2026-11-19-perf-3a-hot-lookups-design.md`

## Global Constraints

- Chỉ sửa `chat-service` (và tài liệu). Không đổi contracts, không đổi schema ScyllaDB.
- Thứ tự trong mọi lệnh: **kiểm định dạng → giới hạn theo người dùng → mới tra store**. `perConv` của `Ingest` giữ **sau** bước tra thành viên (spec §3.2).
- `MemberLookup.of(convId)` **không bao giờ** được gọi cho kênh (spec §3.1).
- Giới hạn API (mỗi người, mỗi pod), cấu hình `chat.limits.api.*`, quá thì `429 rate_limited` + `Retry-After: 1`:
  `GET /v1/conversations` 5/s burst 20; `GET /v1/state` 2/s burst 10; `POST /v1/sync` 2/s burst 10; `GET /v1/presence` 2/s burst 10; lịch sử 20/s burst 50; quản lý (chung) 5/s burst 20; còn lại (chung) 10/s burst 30.
- Badge: hash `badge:{user}`, field `<convId>` = `"<last>:<read>"` (số nguyên thập phân, không dạng mũ), field `_total`, `_built_at` (ms); TTL 30 ngày gia hạn mỗi lần ghi; chỉ DM và nhóm (không kênh); dựng lại khi thiếu hoặc `_built_at` cũ hơn 24 giờ; dựng lại đọc tối đa 10 000 dòng `user_convs`, bỏ dòng kênh.
- `raise` / `drop` không làm gì khi hash không tồn tại, và chạy **sau** lần ghi ScyllaDB tương ứng. Ghi lỗi → `DEL badge:{user}` (best effort), không bao giờ ném ra ngoài fan-out / biên nhận / quản lý thành viên.
- Redis lỗi khi đọc badge → tính bằng cách quét như hiện nay (không làm hỏng thông báo).
- Lệnh build/test: `./mvnw -q -f chat-service test -Dtest=<Class>`; IT: `./mvnw -q -f chat-service verify -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=<ClassIT>` (Docker phải chạy; nếu không: `bash /tmp/claude-0/dockerup.sh`).
- Trước mỗi commit chạy `node .gitnexus/run.cjs detect-changes --scope all --repo .`; trước khi sửa một symbol chạy `node .gitnexus/run.cjs impact "<symbol>" --direction upstream --repo .` và báo HIGH/CRITICAL. Mỗi commit kết thúc bằng hai dòng trailer của session (Co-Authored-By, Claude-Session).

## Review Focus

1. Thành viên vừa bị xoá gửi tin / đánh dấu / gõ phím ngay sau khi bị xoá → bị từ chối ngay (không còn cache nào cho quyền). Test: Task 1 `removedMemberIsRefusedAtOnce` (Ingest, MarkValidator), Task 8 IT.
2. Hash badge bị mất giữa chừng (Redis restart / hết TTL) rồi có tin mới tới trước lần đọc → không tạo hash một phần; lần đọc kế tiếp dựng lại đúng. Test: Task 5 `raiseOnMissingHashCreatesNothing` và property test.
3. Kafka giao lại cùng một tin / cùng một mốc đọc → badge không đổi. Test: Task 5 property test (thao tác "giao lại").
4. Người vào nhóm đã có 100 tin, rồi tin 101 tới → badge tăng đúng 1, không phải 101; kể cả khi `raise` của tin 101 tới trước `raise` của lần vào. Test: Task 5 `joinAfterMessageCountsOne`, Task 7 `addMemberRaisesBadgeAtLastSeq`.
5. Người lạ dồn lệnh vào hội thoại ngẫu nhiên → sau burst `rate_limited` và không còn lần đọc store nào (cả `conversations.get`). Test: Task 1, Task 2, Task 4.

---

### Task 1: Tra thành viên từng người; giới hạn trước tra cho gửi và đánh dấu

**Files:**
- Modify: `chat-service/src/main/java/com/pangate/chat/core/MemberLookup.java`
- Modify: `chat-service/src/main/java/com/pangate/chat/core/Ingest.java` (`check`)
- Modify: `chat-service/src/main/java/com/pangate/chat/core/MarkValidator.java` (`validate`)
- Modify: `chat-service/src/test/java/com/pangate/chat/core/FakeStores.java` (bộ đếm lời gọi)
- Test: `chat-service/src/test/java/com/pangate/chat/core/IngestTest.java`, `MarkValidatorTest.java`

**Interfaces:**
- Produces: `MemberLookup.member(String convId, String userId) -> Optional<ChatStores.Member>` (đọc thẳng `MemberStore.member`, không qua cache). `FakeStores` có `public final AtomicInteger membersCalls, memberCalls, cachedMembersCalls, getCalls` (đếm `members()`, `member()`, `cachedMembers()`, `get()`), và `static FakeStores withChannel(String convId, int followers)` tạo kênh có 1 chủ `"owner"` + `followers` người `"f0".."f{n-1}"`.

- [ ] **Step 1: Viết test fail**

`IngestTest`:
- `channelSendReadsNoMemberList`: `withChannel(C, 100_000)`, `owner` gửi → chấp nhận; `membersCalls == 0`, `cachedMembersCalls == 0`, `memberCalls <= 2`.
- `strangerIsLimitedBeforeAnyLookup`: người `"x"` (không là thành viên) gửi 41 lần tới 41 convId ULID ngẫu nhiên → 40 lần đầu `not_found`, lần 41 `rate_limited`; sau lần 41 `getCalls` và `memberCalls` không tăng.
- `removedMemberIsRefusedAtOnce`: nhóm có `a`, `b`; `a` gửi được; xoá `b` khỏi `fake.members` (giữ nguyên `fake.cache` cũ chứa `b`); `b` gửi → `not_member`.
- `dmToDeletedUserStillRefused`: test hiện có cho `user_deleted` vẫn xanh (không sửa).

`MarkValidatorTest`:
- `channelMarkReadsNoMemberList`: như trên với `validate(f5, ...)` → có lệnh; `membersCalls == 0`, `cachedMembersCalls == 0`, `memberCalls == 1`.
- `strangerIsLimitedBeforeAnyLookup`: 101 dấu tới hội thoại ngẫu nhiên (burst 100) → lần 101 `rate_limited`, `memberCalls` không tăng ở lần đó.
- `removedMemberIsRefusedAtOnce`: như IngestTest → `not_member`.

- [ ] **Step 2: Chạy, thấy fail**

Run: `./mvnw -q -f chat-service test -Dtest='IngestTest,MarkValidatorTest'`
Expected: FAIL — `membersCalls`/`withChannel` chưa có (lỗi biên dịch), rồi sau khi thêm bộ đếm: `membersCalls` = 1, `rate_limited` không xuất hiện.

- [ ] **Step 3: Thêm bộ đếm và `withChannel` vào `FakeStores`; thêm `MemberLookup.member`**

- [ ] **Step 4: Sắp lại `Ingest.check`**

Thứ tự: sender trống → `SendValidator.check` → `perUser.tryAcquire(sender)` → `conversations.get` → `members.member(convId, sender)` → nếu DM: người kia lấy từ `members.of(convId)` (DM ≤ 2 dòng) và `users.isDeleted` → kênh + vai trò `MEMBER` → `perConv.tryAcquire(convId)`.

- [ ] **Step 5: Sắp lại `MarkValidator.validate`**

Thứ tự: định dạng → `perUser.tryAcquire(userId)` → `members.member(conv, userId)` → `lastSeq`.

- [ ] **Step 6: Chạy lại, thấy xanh; chạy cả gói core**

Run: `./mvnw -q -f chat-service test -Dtest='IngestTest,MarkValidatorTest,StateIngestTest,SendControllerTest,StateControllerTest'`
Expected: PASS (test cũ dựa vào thứ tự "not_member trước rate_limited" nếu có thì sửa theo thứ tự mới và ghi Ruling).

- [ ] **Step 7: Commit** — `perf(chat): membership by point read, user limits before lookups for sends and marks`

---

### Task 2: Gõ phím — kiểm loại và giới hạn trước khi tra

**Files:**
- Modify: `chat-service/src/main/java/com/pangate/chat/core/TypingRelay.java`
- Modify: `chat-service/src/main/java/com/pangate/chat/config/ChatProperties.java` (`State`: thêm `@DefaultValue("5") int typingRate, @DefaultValue("10") int typingBurst`)
- Modify: `chat-service/src/main/java/com/pangate/chat/config/ChatConfig.java` (bean `typingRelay`), `chat-service/src/it/java/com/pangate/chat/kafka/ReceiptWorkerIT.java:43` (hàm dựng `State`)
- Test: `chat-service/src/test/java/com/pangate/chat/core/TypingRelayTest.java`

**Interfaces:**
- Consumes: `MemberLookup.member` (Task 1), `FakeStores` bộ đếm.
- Produces: `TypingRelay(ConversationStore, MemberLookup, Pusher, OpenConversations, LongSupplier monoMs, int listedMax, TokenBucketLimiter perUser)`; giá trị trả mới `"rate_limited"`. Giữ hàm dựng 6 tham số cũ, dùng limiter 5/s burst 10 với `System::nanoTime`.

- [ ] **Step 1: Viết test fail**
- `channelTypingReadsNoMemberList`: `withChannel(C, 100_000)`, `f1` gõ → `"channel"`; `membersCalls == 0`, `cachedMembersCalls == 0`, `memberCalls == 0`.
- `strangerIsLimitedBeforeAnyLookup`: limiter 5/s burst 10 với đồng hồ đứng yên; `"x"` gõ 11 lần vào 11 convId ngẫu nhiên → lần 11 `"rate_limited"` và `getCalls` không tăng ở lần đó.
- `nonMemberOfGroupIsRefused`: `"x"` gõ vào nhóm có thật → `"not_member"`, `membersCalls == 0` (chưa tải danh sách).

- [ ] **Step 2: Chạy, thấy fail** — Run: `./mvnw -q -f chat-service test -Dtest=TypingRelayTest`. Expected: FAIL (hàm dựng 7 tham số chưa có; rồi `membersCalls` = 1).

- [ ] **Step 3: Sắp lại `typing`**

`isUlid` → `perUser.tryAcquire(userId)` (sai → `"rate_limited"`) → `conversations.get` (không có → `"not_member"`; kênh → `"channel"`) → `members.member(convId, userId)` (không có → `"not_member"`) → giới hạn 2 s theo (người, hội thoại) → `members.of(convId)` (DM/nhóm) → phần còn lại như cũ. Bean dùng `p.state().typingRate()/typingBurst()`.

- [ ] **Step 4: Chạy lại** — Run: `./mvnw -q -f chat-service test -Dtest=TypingRelayTest` → PASS.

- [ ] **Step 5: Commit** — `perf(chat): typing checks kind and a user limit before any member lookup`

---

### Task 3: Theo dõi online, biên nhận kênh, `lastSeq` kênh

**Files:**
- Modify: `chat-service/src/main/java/com/pangate/chat/core/PresenceWatch.java` (`watch`, `states`, `openable`, `allowed`)
- Modify: `chat-service/src/main/java/com/pangate/chat/core/ReceiptRecipients.java` (thêm `audience`)
- Modify: `chat-service/src/main/java/com/pangate/chat/kafka/ReceiptWorkerLoop.java` (`flushDue`)
- Modify: `chat-service/src/main/java/com/pangate/chat/core/StateService.java` (hàm dựng + `lastSeq` kênh), `ChatConfig.java` (một bean `LastSeqCache` dùng chung cho `MarkValidator` và `StateService`)
- Test: `PresenceWatchTest.java`, `ReceiptRecipientsTest.java`, `StateServiceTest.java`

**Interfaces:**
- Produces: `ReceiptRecipients.audience(ReceiptAggregator.Flush f, MemberLookup members) -> List<Member>`: kênh → mỗi người trong `f.byUser()` thành `Member(user, MEMBER)`, **không** tra; DM/nhóm → `members.of(f.convId())`. `StateService(..., LastSeqCache lastSeq)` (tham số cuối mới).

- [ ] **Step 1: Viết test fail**
- `PresenceWatchTest.watchLooksUpEachConversationOnce`: 10 nhóm, mỗi nhóm có viewer và 20 người đã gửi tin; một watch 200 mục (20 mục mỗi nhóm) → `getCalls <= 10`, `cachedMembersCalls + membersCalls <= 10`, giữ đủ 200 mục.
- `ReceiptRecipientsTest.channelAudienceReadsNoMemberList`: `withChannel(C, 100_000)`, flush kênh có 3 người đọc → `audience` trả 3 người; `membersCalls == 0`, `cachedMembersCalls == 0`; `plan(f, audience, ...)` trả đúng 3 push `own_read_seq`.
- `StateServiceTest.channelLastSeqComesFromCache`: 2 lần `conversationView` cho cùng kênh trong 5 s → `conversations.lastSeq` của kênh được đọc 1 lần (thêm bộ đếm `lastSeqCalls` vào `FakeStores`).

- [ ] **Step 2: Chạy, thấy fail** — Run: `./mvnw -q -f chat-service test -Dtest='PresenceWatchTest,ReceiptRecipientsTest,StateServiceTest'`. Expected: FAIL (`getCalls` = 400, `audience` chưa có, `lastSeqCalls` = 2).

- [ ] **Step 3: Cài đặt**
- `PresenceWatch`: trong một lần `watch` (và một lần `states`) giữ `Map<String, Optional<Set<String>>>` — hội thoại → tập thành viên nếu mở được (DM/nhóm, viewer là thành viên), rỗng nếu không; `openable` / `allowed` đọc từ map này.
- `ReceiptWorkerLoop.flushDue`: `to = ReceiptRecipients.audience(f, members)`; phần ghi mốc và đóng dấu DM giữ nguyên.
- `StateService`: `lastSeq` kênh = `lastSeq.lastSeq(convId)`.

- [ ] **Step 4: Chạy lại cùng lệnh** → PASS.

- [ ] **Step 5: Commit** — `perf(chat): presence watch looks each conversation up once; channel receipts and lastSeq skip the member list`

---

### Task 4: Giới hạn tần suất API HTTP

**Files:**
- Create: `chat-service/src/main/java/com/pangate/chat/core/ApiLimits.java`
- Modify: `ChatProperties.java` (`Limits` thêm thành phần cuối `@DefaultValue Api api`; record `Api` với 14 số nguyên theo thứ tự `listRate, listBurst, stateRate, stateBurst, syncRate, syncBurst, presenceRate, presenceBurst, historyRate, historyBurst, manageRate, manageBurst, otherRate, otherBurst`, mặc định đúng bảng Global Constraints; hằng `Api.DEFAULTS`)
- Modify: `FakeStores.java:21`, `SendValidatorTest.java:12` (thêm `Api.DEFAULTS`)
- Modify: `http/QueryController.java`, `http/StateController.java`, `http/MuteController.java`, `ChatConfig.java` (bean `ApiLimits`), các test dựng controller (`StateControllerTest`, `MuteControllerTest`, `SendControllerTest` nếu dựng `QueryController`)
- Test: Create `chat-service/src/test/java/com/pangate/chat/core/ApiLimitsTest.java`; thêm vào `StateControllerTest`, `MuteControllerTest`

**Interfaces:**
- Produces: `ApiLimits(ChatProperties.Api cfg, LongSupplier nanos)`; `enum ApiLimits.Bucket { LIST, STATE, SYNC, PRESENCE, HISTORY, MANAGE, OTHER }`; `void check(Bucket b, String userId)` ném `ChatRefusal(RATE_LIMITED, ...)`; `static ApiLimits unlimited()` cho test.

Gán bucket: `LIST` = `GET /v1/conversations`; `STATE` = `GET /v1/state`; `SYNC` = `POST /v1/sync`; `PRESENCE` = `GET /v1/presence`; `HISTORY` = `GET .../messages`; `MANAGE` = `POST /v1/conversations`, `POST /v1/dm/{userId}`, `POST .../members`, `DELETE .../members/{userId}`, `PUT .../role`, `POST .../owner`, `POST .../join`; `OTHER` = `GET /v1/conversations/{id}`, `PUT .../mute`, `GET`/`PUT /v1/me/privacy`, `GET .../receipts`. Gọi `check` sau khi kiểm tham số/thân (không đụng store) và trước mọi lời gọi service. `GET .../receipts` không có trong bảng của spec; xếp vào `OTHER` là quyết định của người viết plan (đúng mục tiêu "mọi giới hạn kiểm trước mọi lần tra"); giới hạn 1 lần/s theo (người, hội thoại) hiện có giữ nguyên.

- [ ] **Step 1: Viết test fail**
- `ApiLimitsTest.eachBucketHasItsOwnRateAndBurst`: đồng hồ đứng yên; với mỗi bucket, `burst` lần đầu qua, lần `burst+1` ném `ChatRefusal` code `rate_limited`; sau 1 s (đồng hồ +1e9 ns) qua thêm đúng `rate` lần. Hai bucket `MANAGE` của cùng người dùng chung: 20 lần trộn add/remove → lần 21 bị chặn.
- `ApiLimitsTest.usersDoNotShareBuckets`.
- `StateControllerTest.presenceIsLimitedBeforeLookup`: `ApiLimits` thật, 11 lần `GET /v1/presence` → lần 11 trả 429 và `getCalls` không tăng ở lần đó.
- `MuteControllerTest.muteIsLimited`: 31 lần → 429.

- [ ] **Step 2: Chạy, thấy fail** — Run: `./mvnw -q -f chat-service test -Dtest='ApiLimitsTest,StateControllerTest,MuteControllerTest'`. Expected: FAIL (lớp chưa có).

- [ ] **Step 3: Cài đặt `ApiLimits`** (một `TokenBucketLimiter` mỗi bucket), cấu hình, chèn `check` vào từng handler theo bảng gán.

- [ ] **Step 4: Chạy lại cùng lệnh + `SendControllerTest`** → PASS.

- [ ] **Step 5: Commit** — `feat(chat): per-user rate limits on every read and management API`

---

### Task 5: Badge từ bộ đếm duy trì (lõi + fake)

**Files:**
- Modify: `chat-service/src/main/java/com/pangate/chat/core/ChatStores.java` (thêm port)
- Create: `chat-service/src/main/java/com/pangate/chat/core/BadgeUpdates.java`
- Modify: `chat-service/src/main/java/com/pangate/chat/core/Badge.java`
- Create: `chat-service/src/test/java/com/pangate/chat/core/FakeBadges.java`
- Test: `chat-service/src/test/java/com/pangate/chat/core/BadgeTest.java` (thêm), Create `BadgeCounterPropertyTest.java`

**Interfaces:**
- Produces:
  - `record ChatStores.BadgeRow(String convId, long lastSeq, long readSeq)`.
  - `interface ChatStores.BadgeStore` (mọi phương thức có thể ném `StoreUnavailableException`):
    - `long totalWith(String user, String convId, long seq, long builtAfterMs)` — `-1` khi hash không có hoặc `_built_at < builtAfterMs`; ngược lại áp `raise(user, convId, seq, 0)` (bỏ qua khi `convId == null`) và trả `_total`, trong **một** script.
    - `void rebuild(String user, List<BadgeRow> snapshot, long nowMs)` — gộp: field = max từng phần của field cũ và ảnh chụp; xoá field hội thoại không có trong ảnh chụp; tính lại `_total`; đặt `_built_at = nowMs`.
    - `void raise(String user, String convId, long last, long read)`; `void raiseAll(List<String> users, String convId, long last)` (mỗi người `raise(u, conv, last, 0)`, gửi pipeline); `void drop(String user, String convId)`; `void forget(Collection<String> users)` (`DEL`).
  - `interface BadgeUpdates { void raise(String user, String convId, long last, long read); void raiseAll(List<String> users, String convId, long last); void drop(String user, String convId); BadgeUpdates NONE = ...; }` — không bao giờ ném.
  - `Badge implements BadgeUpdates`: `Badge(MemberStore inbox, BadgeStore counters, LongSupplier wallMs)`; hàm dựng cũ `Badge(MemberStore inbox)` giữ hành vi quét (counters luôn "thiếu", ghi không làm gì). `int total(String user, String convId, long convSeq)`: `totalWith(..., now − 24h)`; `-1` → ảnh chụp `inbox.userConvs(user, null, 10_000)` bỏ dòng kênh → `rebuild` → `totalWith` lại; `StoreUnavailableException` ở bất kỳ bước nào → quét như cũ (cũng bỏ dòng kênh). Ghi lỗi → `counters.forget(những người liên quan)` trong try, nuốt lỗi.

- [ ] **Step 1: Viết `FakeBadges`** — `Map<String, Map<String,String>>` mô phỏng đúng ngữ nghĩa trên (gồm `_total`, `_built_at`), thêm `public boolean down` (mọi lời gọi ném `StoreUnavailableException`), `public int failNext` (N lời gọi kế tiếp ném, rồi bình thường) và `public void lose(String user)` (xoá hash).

- [ ] **Step 2: Viết test fail**

`BadgeTest`:
- `raiseOnMissingHashCreatesNothing`: `raise("u", C, 5, 0)` khi chưa có hash → `FakeBadges` không có `badge:u`.
- `firstReadRebuildsFromInbox`: inbox `u` có DM (last 7, read 4) và kênh (last 9, read 0) → `total("u", null, 0) == 3`, hash có `_built_at`.
- `notifiedMessageCountsBeforeInboxRow`: hash có DM `7:4`; `total("u", DM, 8) == 4`.
- `joinAfterMessageCountsOne`: hash có; `raiseAll([u], G, 101)` rồi `raise(u, G, 100, 100)` → `_total == 1`; đảo thứ tự → vẫn 1.
- `redisDownFallsBackToScan`: `down = true` → `total` bằng kết quả quét, không ném.
- `failedWriteForgetsHash`: hash có; `failNext = 1`; `raise` → không ném, hash bị xoá (lời `forget` sau đó thành công). Với `down = true`: `raise` không ném (cả `forget` cũng lỗi).
- `lostWriteHealsAfter24h`: hash có (built ở t0); ghi inbox DM thêm 2 tin **không** gọi `raise` → `total` lệch 2 ở t0 + 23 h; ở t0 + 24 h + 1 ms → đúng.
- `lostDropHealsAfter24h`: rời nhóm (dòng inbox bị xoá) không gọi `drop` → sau 24 h field nhóm bị xoá, `_total` đúng.

`BadgeCounterPropertyTest.totalAlwaysMatchesInbox` (seed cố định, 200 lần × 300 bước, 3 người, 4 hội thoại gồm 1 kênh): thao tác ngẫu nhiên — tin mới (inbox rồi `raiseAll` trừ người gửi; người gửi `sent` rồi `raise(seq, seq)`), đánh dấu đọc (`advance` rồi `raise(0, read)`), giao lại một thao tác trước đó, vào nhóm (`add` rồi `raise(last, last)`), rời (`remove` rồi `drop`), `lose(user)`; sau mỗi bước: `total(u, null, 0)` == tổng `max(0, last − read)` trên `inbox.userConvs(u)` bỏ kênh.

- [ ] **Step 3: Chạy, thấy fail** — Run: `./mvnw -q -f chat-service test -Dtest='BadgeTest,BadgeCounterPropertyTest'`. Expected: FAIL (biên dịch: `BadgeStore`, `BadgeUpdates` chưa có).

- [ ] **Step 4: Cài đặt port, `BadgeUpdates`, `Badge`** như Interfaces.

- [ ] **Step 5: Chạy lại** → PASS; thêm `OfflineNotificationsTest` → PASS.

- [ ] **Step 6: Commit** — `feat(chat): badge from a maintained per-user counter with merge rebuild`

---

### Task 6: `RedisBadges` (Lua) và IT

**Files:**
- Create: `chat-service/src/main/java/com/pangate/chat/store/RedisBadges.java`
- Test: Create `chat-service/src/it/java/com/pangate/chat/store/RedisBadgesIT.java`

**Interfaces:**
- Consumes: `ChatStores.BadgeStore`, `BadgeRow` (Task 5).
- Produces: `RedisBadges(ChatProperties.Redis cfg) implements ChatStores.BadgeStore, AutoCloseable`, kết nối lười như `RedisPresence` (`call` bọc lỗi thành `StoreUnavailableException`). Script là hằng `String` (như `RedisAttemptCounter`), chạy `EVAL`. `TTL = Duration.ofDays(30)`.

Ghi chú cài đặt: định dạng field bằng `string.format('%d:%d', l, r)` (tránh `1e+15`); `rebuild` lặp `ARGV` (không `unpack`, ảnh chụp tới 10 000 dòng); field hệ thống `_total`, `_built_at` không bao giờ bị coi là hội thoại; `raiseAll` gửi mọi `EVAL` qua `conn.async()` rồi mới chờ (như `onlineOf`), lỗi bất kỳ → `StoreUnavailableException`.

- [ ] **Step 1: Viết IT fail** (`redis:7.4.2` như `RedisPresenceIT`)
- `raiseIsIdempotentAndNeverLowers`: rebuild rỗng; `raise(u,C,10,0)` ×2 → total 10; `raise(u,C,5,3)` → 7; `raise(u,C,0,10)` → 0.
- `raiseAndDropOnMissingHashCreateNothing`: `EXISTS badge:u == 0` sau cả hai.
- `totalWithIsMinusOneWhenMissingOrStale`: chưa có → -1; rebuild ở `now` → `totalWith(..., now+1) == -1`, `totalWith(..., now) >= 0`.
- `rebuildMergesWithMaxAndDropsMissingFields`: hash có `A=10:2`, `B=5:5`; ảnh chụp `A=8:4`, `C=3:0` → `A=10:4`, `B` bị xoá, `C=3:0`, `_total == 9`.
- `largeSeqStaysDecimal`: `raise(u,C,1_000_000_000_000_000L,0)` → field đọc thô `"1000000000000000:0"`.
- `raiseAllPipelinesFiveHundredUsers`: 500 người đã rebuild; `raiseAll` → mỗi người total 1; thời gian < 1 s.
- `ttlIsThirtyDays`: `PTTL` trong (29 ngày, 30 ngày].
- `rebuildOfTenThousandRows`: 10 000 dòng → `_total` đúng.

- [ ] **Step 2: Chạy, thấy fail** — Run: `./mvnw -q -f chat-service verify -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=RedisBadgesIT`. Expected: FAIL (biên dịch).

- [ ] **Step 3: Cài đặt `RedisBadges`.**

- [ ] **Step 4: Chạy lại** → PASS.

- [ ] **Step 5: Commit** — `feat(chat): Redis badge counters (Lua raise, drop, merge rebuild)`

---

### Task 7: Nối badge vào fan-out, biên nhận, quản lý thành viên

**Files:**
- Modify: `kafka/FanOutLoop.java` (hàm dựng + `deliver`), `kafka/ReceiptWorkerLoop.java` (hàm dựng + `flushDue`), `core/Conversations.java` (hàm dựng đầy đủ + `addMember`, `join`, `removeMember`, `leaveDeletedAccount`), `config/ChatConfig.java` (bean `RedisBadges`, `Badge` dùng chung, truyền vào bốn chỗ và `OfflineNotifications`)
- Test: `ConversationsTest.java`; `chat-service/src/it/java/com/pangate/chat/kafka/FanOutIT.java`, `ReceiptWorkerIT.java`

**Interfaces:**
- Consumes: `BadgeUpdates` (Task 5), `RedisBadges` (Task 6).
- Produces: tham số cuối mới `BadgeUpdates badges` cho `FanOutLoop`, `ReceiptWorkerLoop`, hàm dựng đầy đủ của `Conversations`; các hàm dựng cũ truyền `BadgeUpdates.NONE`.

Điểm gọi (đều sau lần ghi ScyllaDB tương ứng):
- `FanOutLoop.deliver`, không phải kênh: sau `touchAll` thành công → `raiseAll(thành viên trừ người gửi, conv, seq)`; sau `senderMarks` thành công → `raise(sender, conv, seq, seq)`. Kênh: không gọi.
- `ReceiptWorkerLoop.flushDue`, không phải kênh: sau khi ghi mốc của một hội thoại → mỗi người có `readSeq > 0`: `raise(user, conv, 0, readSeq)`.
- `Conversations`: thêm vào nhóm (`addMember`, kể cả từ tạo nhóm thì không cần vì `last_seq = 0`) với `c.lastSeq() > 0` → `raise(user, conv, c.lastSeq(), c.lastSeq())`; `removeMember` (cả tự rời) và `leaveDeletedAccount` → `drop(user, conv)`. Kênh: không gọi.

- [ ] **Step 1: Viết test fail**
- `ConversationsTest.addMemberRaisesBadgeAtLastSeq`: nhóm có `lastSeq` 100; `addMember` → `BadgeUpdates` ghi nhận `raise(u, G, 100, 100)` sau `fake.members` đã có `u`.
- `ConversationsTest.leavingDropsBadge`: `removeMember(u, G, u)` → `drop(u, G)`; kênh: không ghi nhận gì.
- `FanOutIT.raisesBadgesAfterInboxRows`: nhóm a,b,c; a gửi → recording `BadgeUpdates` nhận `raiseAll([b,c], G, 1)` và `raise(a, G, 1, 1)`; lúc `raiseAll` được gọi, dòng inbox của b đã có `lastSeq == 1`.
- `ReceiptWorkerIT.readRaisesBadge` / `channelReadRaisesNothing`.

- [ ] **Step 2: Chạy, thấy fail** — Run: `./mvnw -q -f chat-service test -Dtest=ConversationsTest` rồi IT: `./mvnw -q -f chat-service verify -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false -Dit.test='FanOutIT,ReceiptWorkerIT'`. Expected: FAIL (hàm dựng chưa có).

- [ ] **Step 3: Cài đặt** điểm gọi và wiring.

- [ ] **Step 4: Chạy lại cả hai lệnh** → PASS.

- [ ] **Step 5: Commit** — `feat(chat): fan-out, receipts and membership changes keep badge counters`

---

### Task 8: IT thành viên bị xoá, toàn bộ suite, tài liệu

**Files:**
- Modify: `chat-service/src/it/java/com/pangate/chat/http/StateApiIT.java` (hoặc `QueryApiIT`)
- Modify: `chat-service/README.md` (giới hạn API, badge Redis + bộ nhớ, giới hạn đã biết > 10 000 hội thoại, giới hạn theo pod), `CHANGELOG.md`, spec 3a (trạng thái "Đã làm")

- [ ] **Step 1: Viết IT** `removedMemberMarkIsRefusedAtOnce`: thành viên đánh dấu được; bị xoá; `POST .../read` ngay sau → 403 `not_member` (theo ánh xạ hiện có của `StateController`).
- [ ] **Step 2: Chạy** — `./mvnw -q -f chat-service verify -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false -Dit.test='StateApiIT,QueryApiIT'` → PASS (gồm `removedMemberStopsReceivingAtOnce`).
- [ ] **Step 3: Toàn bộ suite** — `./mvnw -q -f chat-service verify > /tmp/claude-0/3a-verify.log 2>&1; tail -40 /tmp/claude-0/3a-verify.log` → BUILD SUCCESS.
- [ ] **Step 4: Tài liệu** theo danh sách Files.
- [ ] **Step 5: Commit** — `docs(chat): perf 3a limits, badge counters and known limits`
