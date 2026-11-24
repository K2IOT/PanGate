# Đợt 3c: vòng Kafka thành pipeline theo khoá — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest, sequencer, fan-out và offline notifier của chat-service không còn chờ partition chậm nhất: mỗi khoá Kafka (hội thoại hoặc người gửi) có một làn riêng, một hội thoại chậm chỉ làm chậm chính nó, lệnh trạng thái chạy ở group riêng, và fan-out ghi inbox của mọi thành viên đồng thời.

**Architecture:** Một lớp `KeyedPipeline` dùng chung: luồng poll chỉ poll, xếp record vào làn `(partition, khoá)`, pause partition quá tải và commit mốc (offset chưa xong nhỏ nhất); mỗi làn là một virtual thread gọi handler của vòng với một mẻ record cùng khoá, lỗi thì thử lại tại chỗ. Bốn vòng hiện có giữ tên lớp, thay thân bằng một handler trên `KeyedPipeline`.

**Tech Stack:** Java 21 (virtual threads), kafka-clients (bản Spring Boot quản lý, có `currentLag`), DataStax/Scylla java-driver 4.19 (`executeAsync`), Micrometer, JUnit 5 + AssertJ + Awaitility, `MockConsumer`.

**Spec:** `docs/superpowers/specs/2026-11-24-perf-3c-kafka-pipelines-design.md`

## Global Constraints

- Không đổi topic, số partition, contract; không thêm thư viện; không thêm topic DLQ (fan-out và offline không có DLQ).
- Thứ tự: theo hội thoại cho `chat.commands`, `chat.events`; theo principal cho topic upstream; theo khoá record cho `cN.push.offline`. Khoá null = một làn chung của partition.
- Không bao giờ commit qua record chưa xong; không bao giờ hai mẻ cùng khoá chạy cùng lúc trên một pod.
- `chat.pipeline.*`: `max-pending-per-partition` 1000 (pause; resume dưới 500), `max-batch` 100 (chỉ sequencer; các vòng khác 1), `max-concurrency` 256, `commit-interval` 200ms, `revoke-wait` 5s. `chat.fanout.inbox-concurrency` 256 (chung cả pod). `chat.kafka.state-group` `chat-state` (`auto.offset.reset=latest`).
- Thử lại tại chỗ: chờ 100 ms → ×2 → tối đa 5 s; mẻ đóng băng; trước mỗi lần thử, làn kiểm partition còn được giao và pipeline chưa đóng.
- Record độc (chỉ pipeline bật DLQ: sequencer, ingest, state): `PoisonGuard.MAX_ATTEMPTS` (5) lỗi tất định liên tiếp theo record đầu mẻ → lần gọi kế là "lần cuối", handler tự DLQ vào `chat.commands.dlq`.
- Metric, tag `pipeline` ∈ `ingest`, `state`, `sequencer`, `fanout`, `offline`: `chat.pipeline.pending`, `chat.pipeline.lanes`, `chat.pipeline.paused` (gauge), `chat.pipeline.batch` (timer), `chat.pipeline.retries{reason=transient|poison}`, `chat.pipeline.oldest` (gauge, ms), `chat.kafka.lag` (gauge). Bỏ `chat.fanout.budget`; giữ metric cũ còn lại.
- Lệnh: unit `./mvnw -q -f chat-service test -Dtest=<Class>`; IT `./mvnw -q -f chat-service verify -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=<ClassIT>` (Docker: `bash /tmp/claude-0/dockerup.sh` nếu chưa chạy).
- Trước khi sửa một symbol: `node .gitnexus/run.cjs impact "<symbol>" --direction upstream --repo .`, báo HIGH/CRITICAL; trước mỗi commit: `node .gitnexus/run.cjs detect-changes --scope all --repo .`. Mỗi commit kết thúc bằng hai dòng trailer của session.

## Review Focus

1. Partition bị thu hồi rồi giao lại cho chính pod này trong lúc một mẻ của khoá K còn chạy → làn mới của K chờ mẻ cũ xong (không hai sequencer cùng hội thoại song song). Test: Task 1 `reassignedPartitionWaitsForTheRunningBatchOfAKey`.
2. Một hội thoại nóng (100 tin/s) → sequencer gom tối đa `max-batch` mỗi lần, giữ thứ tự, không tụt dần. Test: Task 2 `hotConversationIsBatchedInOrder`.
3. Record không có khoá → một làn chung mỗi partition, thứ tự giữ. Test: Task 1 `nullKeysShareOneLanePerPartition`.
4. Store sập cho mọi hội thoại → bộ nhớ có giới hạn: partition pause ở 1000 record chờ, poll vẫn chạy (không bị group đuổi), resume khi store về. Test: Task 1 `pausesAtMaxPendingAndResumesBelowHalf`.
5. Đóng pod khi một làn đang thử lại mãi → `close()` trả về trong `revoke-wait`, commit phần đã xong, không commit qua mẻ đang thử. Test: Task 1 `closeStopsRetryingLanesAndCommitsWhatIsDone`.

---

### Task 0: Số đo trước 3c

**Files:**
- Modify: `chat-service/src/it/java/com/pangate/chat/it/ChatLoadIT.java`

- [ ] **Step 1:** Thêm ca `directMessages100PerSecond` (1-1 qua WebSocket, 100 tin/s trong 10 s, báo p50/p99/max gửi → nhận), `groupOf500At10PerSecond` (một nhóm 500, 10 tin/s trong 10 s, đếm đủ 500 × 100 lần nhận, báo p99 và thời gian tới lần nhận cuối), `fiveGroupsOf500At5PerSecond` (5 nhóm, mỗi nhóm 5 tin/s trong 10 s). Dùng lại `report`, `listen` hiện có; ca không đạt không fail test (đây là số đo).
- [ ] **Step 2: Chạy trên code hiện tại** `./mvnw -q -f chat-service verify -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=ChatLoadIT -Dchat.load=true` → ghi các số vào ledger (dòng `Baseline:`) để Task 6 so.
- [ ] **Step 3: Commit** — `test(chat): load cases for perf 3c (1-1 100/s, group 500 10/s, five groups)`

---

### Task 1: `KeyedPipeline`

**Files:**
- Create: `chat-service/src/main/java/com/pangate/chat/kafka/KeyedPipeline.java`
- Modify: `chat-service/src/main/java/com/pangate/chat/config/ChatProperties.java` (record `Pipeline`, `Fanout`; `Kafka.stateGroup`), `chat-service/src/main/resources/application*.yml` nếu có khoá liệt kê
- Test: `chat-service/src/test/java/com/pangate/chat/kafka/KeyedPipelineTest.java`, `config/ChatPropertiesTest.java`

**Interfaces:**
- Produces:
  - `public final class KeyedPipeline implements AutoCloseable` với `KeyedPipeline(String name, org.apache.kafka.clients.consumer.Consumer<String, byte[]> consumer, String topic, Settings settings, Handler handler, Runnable onRevoked, MeterRegistry meters)`, `void start()` (platform thread `chat-<name>`), `void close()`.
  - `@FunctionalInterface public interface Handler { void handle(List<ConsumerRecord<String, byte[]>> batch, boolean lastTry) throws Exception; }` — mẻ cùng khoá, theo offset; ném lỗi = thử lại đúng mẻ đó.
  - `public record Settings(int maxPendingPerPartition, int maxBatch, int maxConcurrency, Duration commitInterval, Duration revokeWait, boolean deadLetters)` và `static Settings of(ChatProperties.Pipeline p, int maxBatch, boolean deadLetters)`.
  - `ChatProperties.Pipeline(maxPendingPerPartition=1000, maxBatch=100, maxConcurrency=256, commitInterval=200ms, revokeWait=5s)` dưới `chat.pipeline`; `ChatProperties.Fanout(inboxConcurrency=256)` dưới `chat.fanout`; `ChatProperties.Kafka.stateGroup = "chat-state"`.
  - Phân loại lỗi tạm thời: `PoisonGuard.isTransient(Throwable)` (giữ nguyên).

- [ ] **Step 1: Test fail** — `KeyedPipelineTest` với `MockConsumer` (một topic, 2 partition, `rebalance` để giao), handler giả có thể chặn bằng latch, `SimpleMeterRegistry`, settings nhỏ (`maxPending` 4, `revokeWait` 1 s, `commitInterval` 50 ms):
  - `orderIsKeptWithinAKey`: 50 record khoá `a` → handler thấy offset tăng dần, không chồng nhau.
  - `keysOfOnePartitionRunInParallel`: handler của khoá `a` chặn; record khoá `b` cùng partition vẫn được xử lý.
  - `commitNeverPassesAnUnfinishedRecord`: `a`@0 chặn, `b`@1..3 xong → `committed(tp)` ≤ 0 (hoặc chưa có); thả `a` → commit tới 4.
  - `pausesAtMaxPendingAndResumesBelowHalf`: handler chặn mọi khoá; sau 4 record chờ `paused()` chứa partition, `poll` vẫn được gọi (đếm số lần poll tăng), gauge `chat.pipeline.oldest` > 0 và `chat.pipeline.paused` = 1; thả → `paused()` rỗng.
  - `transientFailureIsRetriedInPlaceWithoutSeek`: handler ném `StoreUnavailableException` 3 lần rồi thành công → không có `seek`, mẻ được gọi 4 lần với cùng offset, `chat.pipeline.retries{reason=transient}` = 3.
  - `deterministicFailureGetsALastTryAfterMaxAttempts` (deadLetters=true): `IllegalArgumentException` → lần gọi thứ 5 có `lastTry=true`; với deadLetters=false thì không bao giờ `lastTry`.
  - `retriedBatchIsFrozen`: trong lúc mẻ [0] đang thử lại, record 1 cùng khoá tới → lần thử lại vẫn chỉ [0].
  - `nullKeysShareOneLanePerPartition`: record khoá null giữ thứ tự, không chồng nhau.
  - `maxConcurrencyIsNeverExceeded`: 10 khoá, `maxConcurrency` 3 → tối đa 3 handler cùng lúc.
  - `revokeWaitsForRunningBatchesCommitsAndCallsHook`: thu hồi khi một mẻ đang chạy (thả trong 200 ms) → mốc của mẻ đó được `commitSync` trước khi callback trả, `onRevoked` được gọi.
  - `reassignedPartitionWaitsForTheRunningBatchOfAKey`: mẻ của `a` chạy quá `revokeWait`; partition giao lại; record mới của `a` → handler mới không chạy khi mẻ cũ chưa xong.
  - `closeStopsRetryingLanesAndCommitsWhatIsDone`: khoá `a` lỗi mãi, `b` xong → `close()` trả trong < 2 × `revokeWait`, commit không vượt `a`.
  - `lanesStillRunningAfterRevokeWaitAreInterrupted`: handler tự thử lại bằng `Thread.sleep` trong vòng lặp riêng → sau `revokeWait` (thu hồi hoặc đóng) thread của nó bị interrupt và handler thoát.
  - `ChatPropertiesTest`: giá trị mặc định của `Pipeline`, `Fanout`, `Kafka.stateGroup`.
- [ ] **Step 2: Chạy** `./mvnw -q -f chat-service test -Dtest='KeyedPipelineTest,ChatPropertiesTest'` → FAIL (biên dịch).
- [ ] **Step 3: Cài đặt** theo spec §4.1. Chỉ luồng poll chạm consumer: làn báo offset xong qua một hàng đợi đồng thời, luồng poll áp vào bộ theo dõi offset (tập đang chờ mỗi partition), tính mốc, `commitAsync` mỗi `commitInterval`; `commitSync` trong `onPartitionsRevoked` và `close`. Một sổ đăng ký khoá đang chạy (`(topic, partition, key)` → mẻ) giữ qua rebalance để thoả Review Focus 1. Semaphore `maxConcurrency`. Làn của partition bị thu hồi (hoặc khi đóng) còn chạy sau `revokeWait` thì bị `interrupt()`: đó là cách duy nhất handler có vòng thử lại riêng (fan-out) biết phải dừng; `InterruptedException` không bao giờ làm mất record (offset không được commit). Gauge `chat.kafka.lag` = tổng `consumer.currentLag(tp)` có giá trị.
- [ ] **Step 4: Chạy lại** cùng lệnh → PASS; rồi `./mvnw -q -f chat-service test` → PASS.
- [ ] **Step 5: Commit** — `feat(chat): keyed Kafka pipeline (lanes per key, commit watermark, pause, in-place retry)`

---

### Task 2: Sequencer trên pipeline

**Files:**
- Modify: `chat-service/src/main/java/com/pangate/chat/kafka/SequencerLoop.java`, `config/ChatConfig.java`
- Test: `chat-service/src/test/java/com/pangate/chat/kafka/PoisonRecordTest.java`, mới `SequencerLoopTest.java`; IT `SequencerIT`

**Interfaces:**
- Consumes: `KeyedPipeline`, `Settings.of(p, maxBatch, true)`.
- Produces: `SequencerLoop(Consumer, String topic, Sequencer, EventProducer, SendResults, CommandProducer dlq, MeterRegistry)` (giữ, settings mặc định) và thêm tham số cuối `KeyedPipeline.Settings`. `start()`, `close()` giữ.

- [ ] **Step 1: Test fail** — `SequencerLoopTest` (`MockConsumer`, `FakeStores`, `EventProducer`/`SendResults` giả):
  - `conversationsOfOnePartitionAreSequencedInParallel`: store của hội thoại `c1` chặn trong `reserveSeq`; lệnh của `c2` cùng partition vẫn ra event.
  - `hotConversationIsBatchedInOrder`: 300 lệnh của `c1` → `Sequencer.process` được gọi với mẻ ≤ 100, seq 1..300 đúng thứ tự gửi.
  - `fenceIsRetriedInPlace`: `reserveSeq` trả false một lần → không mất, không trùng seq.
  - `PoisonRecordTest.sequencerDeadLettersOnlyThePoisonCommandAndGoesOn`: chuyển sang pipeline, giữ ý (bỏ kiểm `seek`).
- [ ] **Step 2: Chạy** `./mvnw -q -f chat-service test -Dtest='SequencerLoopTest,PoisonRecordTest'` → FAIL.
- [ ] **Step 3: Cài đặt** handler theo spec §4.2: record hỏng → DLQ ngay; mẻ → `Sequencer.process` → `events.sendAll` → `results.publish`; `lastTry` → từng lệnh một, lệnh lỗi tất định → DLQ (logic `partition(...)` hiện có). `onRevoked` = `sequencer::forget`. Bỏ `step()`, `workers`, `PoisonGuard` riêng của vòng. `ChatConfig.sequencerLoop` truyền `Settings.of(p.pipeline(), p.pipeline().maxBatch(), true)`.
- [ ] **Step 4: Chạy** unit trên + IT `SequencerIT` → PASS.
- [ ] **Step 5: Commit** — `feat(chat): sequencer runs per conversation on the keyed pipeline`

---

### Task 3: Fan-out trên pipeline, inbox theo lô

**Files:**
- Modify: `core/ChatStores.java` (`MemberStore`), `store/ScyllaChatStore.java`, `src/test/.../core/FakeStores.java`, `kafka/FanOutLoop.java`, `config/ChatConfig.java`
- Test: mới `src/test/.../kafka/FanOutLoopPipelineTest.java` (hoặc mở rộng `FanOutLoopTest`), IT `FanOutIT`, `ChatStoreIT`

**Interfaces:**
- Produces: `List<String> MemberStore.touchUserConvs(List<String> userIds, String convId, long seq, long atMs)` — trả về người **chưa ghi được**; default = gọi `touchUserConv` từng người, gom người lỗi. `ScyllaChatStore` cài bằng `executeAsync` của đúng batch hiện có (`touchUserConv` + `stamp`), giới hạn bằng một `Semaphore` của store (`inboxConcurrency`, constructor mới; constructor cũ = 256), chờ tối đa `chat.scylla.timeout` cho cả lô.
- `FanOutLoop` constructors giữ (bỏ các bản có `Duration stepBudget`), thêm bản có `KeyedPipeline.Settings`; bỏ `GIVE_UP`, `STEP_BUDGET`, `consumerConfig` giữ nhưng bỏ `MAX_POLL_INTERVAL_MS` riêng.

- [ ] **Step 1: Test fail:**
  - `FanOutLoopPipelineTest.inboxOf500IsWrittenTogetherAndOnlyFailuresRetried`: `FakeStores` cài `touchUserConvs` lỗi cho 3 người lần đầu → lần hai chỉ gọi với 3 người đó; push không gửi lại.
  - `FanOutLoopPipelineTest.slowConversationDoesNotDelayAnotherOnTheSamePartition`: push của `c1` không trả lời; event của `c2` cùng partition vẫn được push và commit không vượt `c1`.
  - `ChatStoreIT.touchUserConvsWritesEveryRowAndReportsFailures`: 500 người, đọc lại `user_convs` đủ 500 dòng với `last_seq`.
  - `FanOutIT`: xoá `stepBudgetCommitsProgressAndDeliversEachEventOnce` và kiểm `chat.fanout.budget`; `partitionsAreSentInParallel` giữ; thêm `stuckConversationDoesNotDelayAnotherOnTheSamePartition` (hai hội thoại cùng partition, store lỗi theo hội thoại cho `c1`, `c2` tới đủ trong 2 s).
- [ ] **Step 2: Chạy** `./mvnw -q -f chat-service test -Dtest='FanOutLoopPipelineTest'` → FAIL.
- [ ] **Step 3: Cài đặt** theo spec §4.3: handler một event; push như hiện có; `touchUserConvs` lặp với người lỗi, chờ `Thread.sleep` 100 ms → 5 s, không hạn chót, thoát khi bị interrupt (Task 1); push và mark người gửi cũng thử lại trong handler như hiện có, để lần thử lại không gửi lại push đã xong; mark người gửi, badge giữ thứ tự hiện có. `ChatConfig` truyền `p.fanout().inboxConcurrency()` vào `ScyllaChatStore`.
- [ ] **Step 4: Chạy** unit trên + IT `FanOutIT,ChatStoreIT` → PASS.
- [ ] **Step 5: Commit** — `feat(chat): fan-out per conversation on the keyed pipeline, inbox rows written together`

---

### Task 4: Ingest và group `chat-state`

**Files:**
- Modify: `kafka/UpstreamCommandListener.java`, `config/ChatConfig.java` (hai bean, thêm vào thứ tự start/close)
- Test: `PoisonRecordTest`, mới `UpstreamCommandListenerTest.java`; IT `UpstreamCommandListenerIT`

**Interfaces:**
- Produces: `enum UpstreamCommandListener.Role { INGEST, STATE }`; constructor hiện có thêm tham số cuối `Role` (bản cũ = `INGEST`) và bản có `KeyedPipeline.Settings`; `GROUP = "chat-ingest"`, `STATE_GROUP = "chat-state"`; `static KafkaConsumer<String, byte[]> consumer(String bootstrap, String group, String autoOffsetReset)` (bản cũ = `GROUP`, `earliest`).
- INGEST xử lý `chat.send`, `chat.channel.open`, `chat.channel.close`; STATE xử lý `chat.delivered`, `chat.read`, `chat.typing`, `chat.presence.watch`; op của vai kia → `ignored`. Metric `chat.ingest.records` thêm tag `role`.

- [ ] **Step 1: Test fail:**
  - `UpstreamCommandListenerTest.ingestIgnoresStateOps` / `stateIgnoresSendAndChannelOps` (`MockConsumer`, `StateCommands` và `Ingest` giả đếm lời gọi).
  - `UpstreamCommandListenerTest.slowWatchDoesNotDelaySendOfTheSameUser`: hai listener (INGEST, STATE) trên hai `MockConsumer` cùng dữ liệu; `watch` chặn → `chat.send` của cùng principal vẫn được `Ingest.accept`.
  - `UpstreamCommandListenerIT.readOpLandsOnReceiptsTopic`: chạy với listener `STATE`; thêm `stateGroupStartsAtTheEnd` (lệnh read cũ produce trước khi group `chat-state` tồn tại không được xử lý; lệnh mới thì có).
- [ ] **Step 2: Chạy** `./mvnw -q -f chat-service test -Dtest='UpstreamCommandListenerTest,PoisonRecordTest'` → FAIL.
- [ ] **Step 3: Cài đặt** theo spec §4.4: handler một record (khoá principal), `lastTry` → record lỗi tất định vào DLQ (logic `handleAll` hiện có); `ChatConfig` tạo `upstreamCommandListener` (INGEST, `chat-ingest`, earliest) và `stateCommandListener` (STATE, `p.kafka().stateGroup()`, latest), start và close cả hai.
- [ ] **Step 4: Chạy** unit trên + IT `UpstreamCommandListenerIT` → PASS.
- [ ] **Step 5: Commit** — `feat(chat): ingest and state commands on separate keyed pipelines (group chat-state)`

---

### Task 5: Offline notifier trên pipeline

**Files:**
- Modify: `kafka/OfflineNotifierLoop.java`, `config/ChatConfig.java`
- Test: `OfflineNotifierLoopTest` (thay test rewind), IT `OfflineNotifierIT`

**Interfaces:**
- Produces: constructor hiện có giữ, thêm bản có `KeyedPipeline.Settings`; `Settings.of(p, 1, false)`.

- [ ] **Step 1: Test fail:**
  - `OfflineNotifierLoopTest.recipientsAreNotifiedInParallel`: `NotifySink` của người `u1` không ack; record của `u2` cùng partition vẫn được gửi.
  - `OfflineNotifierLoopTest.failedSendIsRetriedInPlace`: sink lỗi 2 lần → gửi lần 3 cùng request id, không seek. (Xoá `keepsRunningWhenTheRewindTimesOut`: không còn rewind.)
- [ ] **Step 2: Chạy** `./mvnw -q -f chat-service test -Dtest=OfflineNotifierLoopTest` → FAIL.
- [ ] **Step 3: Cài đặt** theo spec §4.5: handler = `handle` → `sink.send(q).get(produce timeout)`; record hỏng đếm `invalid` và bỏ qua như hiện có.
- [ ] **Step 4: Chạy** unit trên + IT `OfflineNotifierIT` → PASS.
- [ ] **Step 5: Commit** — `feat(chat): offline notifier on the keyed pipeline`

---

### Task 6: Đo, tài liệu, kiểm toàn bộ

**Files:**
- Modify: `chat-service/README.md` (bảng số đo, `chat.pipeline.*`, `chat.fanout.inbox-concurrency`, `chat.kafka.state-group`, metric mới, bỏ `chat.fanout.budget`), `CHANGELOG.md` (mục "Perf 3c"), spec (trạng thái "Đã làm", số đo)

- [ ] **Step 1: Chạy** `ChatLoadIT` như Task 0 trên code mới; so với dòng `Baseline:` của ledger. Mục tiêu spec §2: 1-1 100 tin/s p99 < 300 ms; nhóm 500 ở 10 tin/s đủ 50 000 lần nhận, lag không tăng dần; 5 nhóm × 5 tin/s đủ. Không đạt → ghi số và hỏi chủ dự án (spec §7), không tự nới thứ tự.
- [ ] **Step 2: Cập nhật** README, CHANGELOG, spec với số trước/sau.
- [ ] **Step 3: Chạy** `./mvnw -q -f chat-service verify` → xanh; `make down bootstrap images up smoke` → `SMOKE OK`.
- [ ] **Step 4: Commit** — `docs: perf 3c measurements, pipeline config and metrics`
