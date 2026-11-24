# Đợt 3b: client Push API — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Client Push API của chat-service gửi qua một pool stream trải trên mọi pod Push API, không bao giờ vượt cửa sổ của server, không còn bão gửi lại, và khi quá tải thì bỏ push ít quan trọng trước, không bao giờ bỏ tin nhắn vì thiếu chỗ.

**Architecture:** `PushApiClient` giữ một `ManagedChannel` (`round_robin` trên DNS) và N `PushStream`. Mỗi `PushStream` là một `SendStream` dùng lâu dài với cửa sổ riêng, hàng đợi theo ba mức (MUST / BEST_EFFORT / DROPPABLE) và id trên dây riêng cho mỗi lần ghi. Push chọn stream theo hash của đích. Push API đặt tuổi tối đa cho kết nối để client nối lại và tìm pod mới.

**Tech Stack:** Java 21, grpc-java 1.85.1 (netty-shaded, grpc-inprocess cho test), Micrometer, JUnit 5 + AssertJ + Awaitility.

**Spec:** `docs/superpowers/specs/2026-11-24-perf-3b-push-api-client-design.md`

## Global Constraints

- Không đổi contract (`PushRequest`, `PushResponse`); **không** đặt `idempotency_key` (spec §4.4).
- Phía Push API chỉ thêm tuổi tối đa của kết nối (spec §4.2).
- Cấu hình `chat.push-api.*`: `streams` 4, `window` 1 000, `low-share` 0.2, `low-wait` 2s, `queue-max` 10 000, `must-wait` 10s, `timeout` 8s (từ lúc ghi). Push API `push-api.grpc.max-connection-age` 30m, `max-connection-age-grace` 30s.
- Mức: MUST = tin nhắn DM/nhóm, BROADCAST, CONTROL (mở/đóng/thu hồi kênh); BEST_EFFORT = biên nhận, báo gửi bị từ chối; DROPPABLE = gõ phím, online (chuyển trạng thái và ảnh chụp watch).
- BEST_EFFORT + DROPPABLE cộng lại ≤ `low-share` × `window` chỗ trên một stream; MUST dùng được cả cửa sổ; khi có chỗ MUST ra trước.
- Khoá stream = `"topic:" + topic` cho BROADCAST, ngược lại `principal_id` (như `PushGrpcService.targetKey`).
- Metric: `chat.push.inflight{stream}`, `chat.push.queued{priority}`, `chat.push.queue.wait{priority}`, `chat.push.dropped{priority,reason}` (`window_full`, `waited`, `queue_full`, `must_wait`), `chat.push.latency`, `chat.push.stream.restarts`.
- Lệnh: unit `./mvnw -q -f chat-service test -Dtest=<Class>`; IT `./mvnw -q -f <module> verify -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=<ClassIT>` (Docker: `bash /tmp/claude-0/dockerup.sh` nếu chưa chạy).
- Trước khi sửa một symbol: `node .gitnexus/run.cjs impact "<symbol>" --direction upstream --repo .`, báo HIGH/CRITICAL; trước mỗi commit: `node .gitnexus/run.cjs detect-changes --scope all --repo .`. Mỗi commit kết thúc bằng hai dòng trailer của session.

## Review Focus

1. Push API trả lời chậm hơn `timeout` một cách đều đặn (produce Kafka chậm) → client không bao giờ ghi vượt cửa sổ, và request MUST trong hàng không hết hạn vì chờ. Test: Task 2 `timedOutRequestKeepsItsSlot`.
2. Nơi gọi gửi lại đúng `PushRequest` (cùng `request_id`) trong khi lần đầu chưa được trả lời → trả lời của lần đầu không hoàn tất future của lần hai. Test: Task 2 `lateAnswerNeverCompletesARetry`.
3. Push API sập hẳn (không mở được stream) → MUST lỗi sau `must-wait`, mở lại có backoff, CPU không quay vòng. Test: Task 3 `mustFailsAfterMustWaitWhenNoStreamOpens`, `reopenBacksOff`.
4. Một trận gõ phím 10 000 lần khi cửa sổ trống → không quá 200 chỗ bị chiếm, tin nhắn đến sau vẫn được ghi ngay. Test: Task 2 `lowPrioritiesNeverExceedTheirShare`.
5. Pod Push API mới, hoặc một pod khởi động lại → stream chuyển sang trong vòng tuổi kết nối, push không mất. Test: Task 5 `connectionAgeEndsStreamsAndClientReconnects`.

---

### Task 1: Mức ưu tiên trên `Pusher` và ở nơi gọi

**Files:**
- Modify: `chat-service/src/main/java/com/pangate/chat/core/Pusher.java`
- Create: `chat-service/src/main/java/com/pangate/chat/core/PushDroppedException.java`
- Modify: `core/TypingRelay.java`, `core/PresenceFanOut.java`, `core/PresenceWatch.java`, `core/PushNotifier.java`, `kafka/ReceiptWorkerLoop.java`, `config/ChatConfig.java` (wrapper `counted`), `grpc/PushApiClient.java` (tạm: nhận và bỏ qua mức)
- Modify (lambda): `src/test/.../core/PresenceWatchTest.java`, `ChannelSubscriptionsTest.java`, `TypingRelayTest.java`, `src/it/.../kafka/FanOutIT.java` và mọi lambda `Pusher` khác (`grep -rn "Pusher" chat-service/src`)
- Test: `TypingRelayTest`, `PresenceWatchTest`, `ReceiptRecipientsTest` hoặc test mới `PusherPrioritiesTest`

**Interfaces:**
- Produces: `enum Pusher.Priority { MUST, BEST_EFFORT, DROPPABLE }`; phương thức trừu tượng `CompletableFuture<PushResponse> send(PushRequest r, Priority p)`; `default send(PushRequest r)` = `send(r, MUST)`. `PushDroppedException extends RuntimeException` (không kế thừa `PushUnavailableException`), có `reason()` ∈ `window_full`, `waited`, `queue_full`.

- [ ] **Step 1: Test fail** — một `Pusher` ghi lại `(type header, priority)`:
  - `TypingRelayTest.typingIsDroppable`: mọi push gõ phím có `DROPPABLE`.
  - `PresenceWatchTest.snapshotAndTransitionsAreDroppable`: ảnh chụp của `watch` và push của `PresenceFanOut.on` có `DROPPABLE`.
  - `PusherPrioritiesTest.notifierIsBestEffort`: `PushNotifier.rejected` gửi `BEST_EFFORT`.
  - `PusherPrioritiesTest.defaultSendIsMust`: `send(r)` gọi `send(r, MUST)`.
- [ ] **Step 2: Chạy** `./mvnw -q -f chat-service test -Dtest='TypingRelayTest,PresenceWatchTest,PusherPrioritiesTest'` → FAIL (biên dịch).
- [ ] **Step 3: Cài đặt** interface, exception; đổi nơi gọi: TypingRelay/PresenceFanOut/PresenceWatch → DROPPABLE; ReceiptWorkerLoop, PushNotifier → BEST_EFFORT; FanOutLoop, ChannelSubscriptions giữ `send(r)` (MUST). Wrapper `counted` trong `ChatConfig.presenceWatch` cài `send(r, p)` và chuyển tiếp `p`. `PushApiClient` tạm cài `send(r, p)` bằng đường cũ.
- [ ] **Step 4: Chạy lại** cùng lệnh + toàn bộ `./mvnw -q -f chat-service test` → PASS.
- [ ] **Step 5: Commit** — `feat(chat): push priorities (must, best effort, droppable) at every caller`

---

### Task 2: `PushStream` — cửa sổ, hàng đợi theo mức, timeout từ lúc ghi

**Files:**
- Create: `chat-service/src/main/java/com/pangate/chat/grpc/PushStream.java`
- Create: `chat-service/src/test/java/com/pangate/chat/grpc/ScriptedPushService.java` (server giả in-process: giữ request, trả lời khi test bảo, đếm số đang chờ, huỷ / kết thúc stream theo lệnh)
- Test: Create `chat-service/src/test/java/com/pangate/chat/grpc/PushStreamTest.java`

**Interfaces:**
- Consumes: `Pusher.Priority`, `PushDroppedException`, `PushUnavailableException`.
- Produces: `final class PushStream implements AutoCloseable` (package-private), `PushStream(Supplier<ClientCall/Stub> opener, PushStream.Limits limits, LongSupplier nanos, ScheduledExecutorService timers, MeterRegistry meters, int index)`; `CompletableFuture<PushResponse> send(PushRequest r, Pusher.Priority p)`; `record Limits(int window, double lowShare, Duration lowWait, int queueMax, Duration mustWait, Duration timeout)`. (Tên tham số opener là lựa chọn của người làm; nó phải cho phép mở lại stream.)

Hành vi (spec §4.1, §4.3):
- Ghi khi `inFlight < window` và (MUST, hoặc `lowInFlight < floor(window × lowShare)`) và `isReady()`; không thì vào hàng theo mức (DROPPABLE: bỏ ngay `window_full`).
- Mỗi lần ghi đặt `request_id` trên dây mới (`Ids.next()`), ghép trả lời theo id đó, trả `PushResponse` với `request_id` gốc.
- Timer `timeout` bắt đầu lúc ghi; hết giờ: future lỗi `PushUnavailableException`, **chỗ vẫn giữ** tới khi server trả lời hoặc stream kết thúc.
- Có chỗ: lấy MUST trước, rồi BEST_EFFORT (nếu còn phần thấp); BEST_EFFORT trong hàng quá `low-wait` → `PushDroppedException("waited")`; hàng đầy → MUST `PushUnavailableException`, BEST_EFFORT `PushDroppedException("queue_full")`.

- [ ] **Step 1: Test fail** (`window` 10, `lowShare` 0.2 trong test để nhỏ):
  - `writesAtMostWindowAndQueuesTheRest`: 15 MUST, server không trả lời → server thấy đúng 10; trả lời 1 → thấy 11.
  - `queuedRequestDoesNotExpire`: `timeout` 100 ms; request thứ 11 nằm trong hàng 1 s vẫn chưa lỗi; được ghi khi có chỗ.
  - `timedOutRequestKeepsItsSlot`: 10 MUST ghi, chờ quá `timeout` → 10 future lỗi `PushUnavailableException`; request 11 **không** được ghi cho tới khi server trả lời một trong 10.
  - `lateAnswerNeverCompletesARetry`: gửi `r`, hết giờ, gửi lại đúng `r`; server trả lời lần đầu → future lần hai chưa hoàn tất; trả lời lần hai → hoàn tất với `request_id` gốc.
  - `droppableIsDroppedWhenLowShareIsFull`: 2 BEST_EFFORT đang chờ → DROPPABLE thứ nhất `PushDroppedException("window_full")`.
  - `bestEffortWaitsThenIsDropped`: cửa sổ đầy MUST; BEST_EFFORT lỗi `waited` sau ≈ `low-wait`.
  - `mustGoesBeforeBestEffort`: cửa sổ đầy; xếp B (BEST_EFFORT) rồi M (MUST); trả lời một → M được ghi trước B.
  - `lowPrioritiesNeverExceedTheirShare`: cửa sổ trống, 1 000 DROPPABLE + BEST_EFFORT → server thấy ≤ 2; rồi 8 MUST → cả 8 được ghi ngay.
  - `queueFullRefusesAtOnce`: `queueMax` 5 → MUST thứ 16 lỗi `PushUnavailableException` ngay; BEST_EFFORT `queue_full`.
- [ ] **Step 2: Chạy** `./mvnw -q -f chat-service test -Dtest=PushStreamTest` → FAIL (biên dịch).
- [ ] **Step 3: Cài đặt** `PushStream` (một khoá cho trạng thái của stream; ghi lên `StreamObserver` trong khoá như hiện nay; `setOnReadyHandler` gọi lại bước lấy khỏi hàng); đăng ký metric `chat.push.inflight`, `queued`, `queue.wait`, `dropped`, `latency` như Global Constraints, và thêm vào test `metricsCountDropsByReason`: một DROPPABLE bị bỏ → `chat.push.dropped{priority=droppable,reason=window_full}` = 1).
- [ ] **Step 4: Chạy lại** → PASS.
- [ ] **Step 5: Commit** — `feat(chat): push stream with client-side window, priority queues and timeout from write`

---

### Task 3: Vòng đời stream — đứt, mở lại, `must-wait`

**Files:**
- Modify: `grpc/PushStream.java`
- Test: `PushStreamTest`

**Interfaces:**
- Consumes: Task 2.
- Produces: không thêm API công khai.

Hành vi (spec §4.2): stream kết thúc (lỗi / `onCompleted`) → request đang chờ trả lời lỗi `PushUnavailableException` ngay, chỗ được trả; hàng giữ nguyên; nếu hàng còn request thì mở lại ngay, thất bại liên tiếp chờ 100 ms → ×2 → tối đa 5 s; MUST trong hàng quá `must-wait` → `PushUnavailableException` (`must_wait`).

- [ ] **Step 1: Test fail:**
  - `endedStreamFailsInFlightAndResendsQueued`: 10 ghi + 5 trong hàng; server huỷ stream → 10 lỗi ngay; 5 được ghi trên stream mới (server thấy stream thứ hai).
  - `mustFailsAfterMustWaitWhenNoStreamOpens`: opener luôn lỗi; `mustWait` 300 ms → MUST lỗi trong 300–600 ms.
  - `reopenBacksOff`: opener luôn lỗi trong 2 s → số lần mở ≤ 6 (100+200+400+800+…), không phải hàng trăm.
  - `idleStreamIsNotReopenedUntilNeeded`: stream kết thúc khi hàng rỗng → không mở lại cho tới lần `send` kế tiếp.
- [ ] **Step 2: Chạy** `./mvnw -q -f chat-service test -Dtest=PushStreamTest` → FAIL.
- [ ] **Step 3: Cài đặt** (mỗi lần mở lại tăng `chat.push.stream.restarts`).
- [ ] **Step 4: Chạy lại** → PASS.
- [ ] **Step 5: Commit** — `feat(chat): push stream reopens with backoff; queued musts fail after must-wait`

---

### Task 4: `PushApiClient` thành pool; cấu hình; nối dây

**Files:**
- Modify: `grpc/PushApiClient.java`, `config/ChatProperties.java` (`PushApi`: thêm `streams`, `window`, `lowShare`, `lowWait`, `queueMax`, `mustWait`; `timeout` mặc định 8s), `config/ChatConfig.java` (bean `pushApiClient` nhận `MeterRegistry`)
- Modify: `src/it/.../grpc/PushApiClientIT.java`, `src/it/.../kafka/FanOutIT.java` (hàm dựng mới)
- Test: Create `chat-service/src/test/java/com/pangate/chat/grpc/PushApiClientTest.java`

**Interfaces:**
- Consumes: `PushStream`, `PushStream.Limits`.
- Produces: `PushApiClient(ManagedChannel channel, ChatProperties.PushApi cfg, MeterRegistry meters)`; `static PushApiClient create(ChatProperties.PushApi cfg, MeterRegistry meters)` dùng `NettyChannelBuilder.forTarget(target).defaultLoadBalancingPolicy("round_robin")`; giữ `PushApiClient(ManagedChannel, Duration timeout)` cho IT (các giá trị khác mặc định). `static String streamKey(PushRequest r)`.

- [ ] **Step 1: Test fail:**
  - `sameTargetAlwaysUsesTheSameStream`: 1 000 request, 100 người nhận → mỗi người nhận chỉ xuất hiện trên một stream của server giả; người nhận trải trên cả 4 stream.
  - `orderPerRecipientIsKept`: 4 thread, mỗi thread gửi 200 push cho cùng người nhận "p" với số thứ tự → server thấy số thứ tự của từng thread tăng dần.
  - `broadcastKeyIsTheTopic`: hai BROADCAST cùng topic đi cùng stream; `streamKey` = `"topic:<topic>"`.
  - `streamsSpreadOverServers`: hai server Netty trên `127.0.0.1:0` sau một `NameResolverProvider` test trả cả hai địa chỉ, kênh `round_robin` → cả hai server nhận ít nhất một stream.
- [ ] **Step 2: Chạy** `./mvnw -q -f chat-service test -Dtest=PushApiClientTest` → FAIL.
- [ ] **Step 3: Cài đặt** pool (stream mở lười; `index = floorMod(streamKey.hashCode(), streams)`), cấu hình, bean.
- [ ] **Step 4: Chạy lại** + `./mvnw -q -f chat-service test` → PASS; IT `-Dit.test='PushApiClientIT,FanOutIT'` → PASS.
- [ ] **Step 5: Commit** — `feat(chat): Push API client pools streams over round-robin, keyed by target`

---

### Task 5: Biên nhận không gửi lại push bị bỏ; tuổi kết nối ở Push API

**Files:**
- Modify: `kafka/ReceiptWorkerLoop.java` (`push`: `PushDroppedException` là kết quả cuối, đếm `chat.receipts.pushes{result=dropped}`; `ROUND_DEADLINE` = `low-wait` + `timeout` + 1 s, đọc từ cấu hình)
- Modify: `push-api-service/src/main/java/com/pangate/pushapi/config/PushApiProperties.java` (`Grpc`: `@DefaultValue("30m") Duration maxConnectionAge`, `@DefaultValue("30s") Duration maxConnectionAgeGrace`), `grpc/GrpcServerLifecycle.java` (`NettyServerBuilder.maxConnectionAge(...)`, `maxConnectionAgeGrace(...)`; grpc-java tự thêm ±10% ngẫu nhiên), các nơi dựng `PushApiProperties.Grpc` trong test
- Test: `chat-service/src/it/.../kafka/ReceiptWorkerIT.java`; `push-api-service/src/it/.../grpc/SendStreamIT.java`

**Interfaces:**
- Consumes: `PushDroppedException` (Task 1), client mới (Task 4).

- [ ] **Step 1: Test fail:**
  - `ReceiptWorkerIT.droppedPushIsNotResent`: pusher giả trả `PushDroppedException` cho mọi push → mỗi push được gửi đúng một lần; `chat.receipts.pushes{result=dropped}` tăng.
  - `ReceiptWorkerIT.unavailableIsStillRetried`: lần đầu `CODE_UNAVAILABLE`, lần hai OK → gửi hai lần (hành vi cũ giữ).
  - `SendStreamIT.connectionAgeEndsStreamsAndClientReconnects`: Push API với `max-connection-age` 2 s, grace 1 s; `PushApiClient` gửi đều trong 6 s → mọi push OK, `chat.push.stream.restarts` ≥ 1. (Nếu `SendStreamIT` không có client chat, đặt test này ở `chat-service` `PushApiClientIT`, nơi Push API đã chạy trong JVM.)
- [ ] **Step 2: Chạy** các IT trên → FAIL.
- [ ] **Step 3: Cài đặt.**
- [ ] **Step 4: Chạy lại** → PASS.
- [ ] **Step 5: Commit** — `feat: receipts do not resend dropped pushes; Push API caps gRPC connection age`

---

### Task 6: Tài liệu, toàn bộ suite, smoke

**Files:**
- Modify: `chat-service/README.md` (bảng `push-api.*`, ba mức, metric, **headless service** cho `push-api` ở Kubernetes), `push-api-service/README.md` (`grpc.max-connection-age`), `CHANGELOG.md`, spec 3b (trạng thái "Đã làm")

- [ ] **Step 1:** `./mvnw -q -f chat-service verify > /tmp/claude-0/3b-verify.log 2>&1; tail -30 /tmp/claude-0/3b-verify.log` → BUILD SUCCESS; `./mvnw -q -f push-api-service verify` → BUILD SUCCESS.
- [ ] **Step 2:** `make bootstrap images up smoke` → `SMOKE OK`.
- [ ] **Step 3:** Tài liệu theo danh sách Files.
- [ ] **Step 4: Commit** — `docs: perf 3b push priorities, stream pool and connection age`
