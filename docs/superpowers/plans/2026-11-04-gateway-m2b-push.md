# Gateway M2b (Push, broadcast, kick) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hoàn tất M2: gateway nhận `PushBatch` từ Push Router qua gRPC `:9090`, giao unicast/RPC theo delivery class với `PendingAckWindow` và trả `DeliveryAck`. Thêm SUBSCRIBE và broadcast có pacing, control push (subscribe, unsubscribe, kick, refresh). Thêm chế độ single-session với `KickClient` gọi Push API. Devtools có `fake-router` và `fake-pushapi`, và `make smoke` chạy hết vòng connect → upstream → push → ACK → `DeliveryAck` → disconnect.

**Architecture:**
- **Đường nhận:** gRPC (`grpc-netty-shaded`) chạy trên event loop và executor riêng (`gw-grpc-*`). Executor này chỉ decode và chia batch theo loop: mỗi loop nhận **một** `LoopBatchTask`.
- **Trên owner loop:** mọi thao tác với `Session`, `PendingAckWindow`, `TopicIndex` và `BroadcastPacer` đều chạy trên loop sở hữu, đúng mô hình shared-nothing của M1/M2a.
- **Đường ack:** ack đi ngược qua `AckBatcher` của từng stream. Chỉ task flush của batcher, chạy trên executor gRPC, được gọi `onNext`.
- **Broadcast:** frame được encode **một lần** thành `PreEncodedFrame` (ByteBuf direct, refCnt = số loop). Pacer của mỗi loop ghi `retainedDuplicate()`.

**Tech Stack:** Như M2a, thêm gRPC Java (`grpc-netty-shaded`, `grpc-protobuf`, `grpc-stub`, `grpc-services` chỉ trong devtools, `grpc-inprocess` cho test), cùng plugin `protoc-gen-grpc-java` qua `io.github.ascopes:protobuf-maven-plugin`.

**Spec:** `docs/superpowers/specs/2026-11-03-gateway-service-design.md`. Plan này hiện thực nửa sau của **M2** (spec §12), tiếp nối `docs/superpowers/plans/2026-11-03-gateway-m2a-cluster-infra.md`. Các mục chính: §5.3, §5.4, §5.7, §5.8, §6.3–§6.6, §2.3 (`fake-router`, `fake-pushapi`), §2.4 (`fake-pushapi` trong compose) và §9.1 (`make smoke`).

## Global Constraints

**Kế thừa và version**
- Mọi ràng buộc trong "Global Constraints" của plan M1 và M2a vẫn giữ nguyên: không block trên `gw-io-*`, callback quay về owner loop qua `session.loop().eventLoop().execute`, `-Dpangate.assertLoop=true` trong test, BlockHound, Netty `4.2.20.Final`, metric dạng chấm, Conventional Commits, impact analysis trước khi sửa symbol, `detect-changes` trước khi commit.
- gRPC: property `grpc.version` trong `platform-parent`, đặt **`1.76.0`**. Nếu Maven Central đã có bản patch `1.76.x` mới hơn, dùng bản đó. Import `io.grpc:grpc-bom:${grpc.version}` vào `dependencyManagement`. `grpc-netty-shaded` mang Netty riêng đã shade, nên không vi phạm quy tắc Netty `4.2.20.Final` của data plane. Không thêm `spring-grpc` hay `grpc-spring-boot-starter`.

**Contract với router (spec §6.3)**
- Enum dùng tên buf STANDARD đã có trong `router.proto` (M2a Task 1), tức là `KIND_MESSAGE`, `STATUS_ACKED`, v.v. `PRIORITY_UNSPECIFIED` được xử lý như `NORMAL`. `DELIVERY_CLASS_UNSPECIFIED` được xử lý như `EPHEMERAL`.
- Getter Java của field `class` trong `Frame` là `getClass_()`.
- Header gRPC `pangate-push-version`: hằng `CURRENT = 1`, chấp nhận `{CURRENT, CURRENT − 1} ∩ [1, ∞)` (hiện tại chỉ có `"1"`). Thiếu header hoặc giá trị khác → `FAILED_PRECONDITION`.
- Hằng số: pending window 32 msg / 64 KB (`gateway.limits.pending-ack.{msgs,bytes,timeout}`, timeout 5 s). `ACK` tối đa 32 id, vượt → close 4000. `NACK_OVERFLOW` ≥ 64 lần trong 30 s → close 4008.

**gRPC server**
- Port `gateway.grpc.port` (9090), phase `MAX − 1700`.
- Event loop: 2 thread `gw-grpc-el-*`. Executor: `Executors.newFixedThreadPool(2)` với thread `gw-grpc-exec-*`.
- Flow control: `disableAutoRequest`, `request(8)` lúc đầu, `request(1)` sau mỗi batch khi `shedLevel < 4`. Stop: `shutdown()`, chờ 5 s, rồi `shutdownNow()`.

**AckBatcher và broadcast**
- AckBatcher: `ConcurrentLinkedQueue<AckItem>`. Flush mỗi 2 ms hoặc khi hàng đợi ≥ 256. Mỗi `DeliveryAck` chứa tối đa 256 `AckItem`.
- Broadcast:
  - Tốc độ `gateway.broadcast.rate-per-loop` (10000/s), nghĩa là mỗi 10 ms ghi `rate/100` recipient.
  - `CRITICAL` ghi ngay. `NORMAL` được ưu tiên trước `BULK`.
  - `shedLevel ≥ 1` → dừng `BULK`. Job vẫn nằm trong hàng đợi, không bị drop.
  - Kill switch `broadcastEnabled=false` của tenant → drop mọi job khác `CRITICAL`.
- Pacer chỉ lên lịch timer 10 ms khi còn job: loop rỗi không có timer.

**Topic**
- Topic phải có tiền tố `<tenantId>:`.
- Pattern trong JWT: `*` khớp mọi chuỗi ký tự, kể cả rỗng và kể cả `:`. Mọi ký tự khác so khớp nguyên văn.
- Giới hạn `topicsPerConn` lấy từ `TenantPolicy`, mặc định 100.

**Single-session và KICK (spec §6.6)**
- Entry cũ trên chính node (cùng `nodeId` **và** cùng `epoch`) → kick local.
- Entry trên node khác → `KickClient` gửi `PushRequest{KIND_CONTROL, control=KICK}`. Retry 2 lần sau 100 ms rồi 400 ms, sau đó bỏ và tăng `gw.kick.failures`. Mỗi lần gửi chờ `PushResponse` tối đa 2 s.
- `gateway.push-api.enabled=false` (local) → entry ở node khác chỉ bị bỏ qua, ghi WARN.

**Bảo mật gRPC**
- Prod: mTLS. `SpiffeAuthInterceptor` so URI SAN của client cert với `gateway.grpc.allowed-spiffe` (danh sách), sai → `UNAUTHENTICATED`.
- `gateway.grpc.plaintext=true` chỉ dùng trong profile `local` và `dev`.
- `KickClient` dùng cùng cert `gateway.grpc.tls.*` làm client cert khi `gateway.push-api.plaintext=false`.

**Metric** (spec §8)
- `gw.push.handoff` (Timer), `gw.delivery.acks{status}`, `gw.pending.ack.inflight` (gauge), `gw.push.dropped{reason}`.
- `gw.broadcast.recipients{priority}`, `gw.pushstream.active` (gauge), `gw.kick.failures`.
- `gw.control.push{op}`, `gw.subscribe.rejected{reason}`.

**Bộ nhớ:** `Session` chỉ được thêm field lười, `null` khi idle (`pending`, `topics`), cộng tối đa 2 field số nguyên. Heap/session vẫn phải < 5120 B (`make memory-check`; mức hiện tại là 4663 B).

## Review Focus

1. **Router đẩy tới connId đã đóng, sai loop, sai định dạng, hoặc của session cùng sessionKey nhưng khác connId** (session đã tái dùng slot).
   - Hành vi mong đợi: RELIABLE/ORDERED nhận đúng một `NACK_NOT_FOUND`, EPHEMERAL bị drop. Không exception, không ack trùng.
   - Test: Task 7 `messageToUnknownOrMalformedConnIdIsNackedNotFound`; Task 3 `wrongConnIdForSameKeyIsNotFound`.
2. **Stream gRPC đứt khi còn pending.** Ack muộn từ client không gây exception, không gọi `onNext` trên stream chết. Session vẫn mở. Timeout và close sau đó không gửi gì.
   - Test: Task 2 `deadStreamDropsAcksSilently`; Task 8 `streamCancelMarksRefDead`.
3. **Client đóng kết nối khi broadcast đang được pace, hoặc broadcast tới loop không có subscriber.** Không leak `ByteBuf` (leak detection `paranoid` trong test) và không ghi vào channel đã đóng.
   - Test: Task 5 `closedRecipientIsSkippedAndBufferReleased`, `loopWithoutSubscribersReleasesItsShare`.
4. **Client gửi ACK cho msgId lạ, ACK trùng, hoặc ACK sau timeout.** Bỏ qua, không gửi ack thứ hai cho cùng `delivery_id`.
   - Test: Task 3 `unknownAndDuplicateAckIdsAreIgnored`, `ackAfterTimeoutSendsNothing`.
5. **Hai HELLO gần như đồng thời của cùng principal ở tenant single-session**, có thể trên cùng node hoặc khác node. Chỉ session cũ hơn bị đóng 4012, session mới không tự kick mình. KICK tới muộn không đóng session mới hơn `connected_at_ms`.
   - Test: Task 6 `kickSparesNewConnAndNewerSessions`; Task 9 `localPreviousEntryKickedRemoteSentToPushApi`.

---

## File Structure

```
platform-parent/pom.xml                        # + grpc.version, grpc-bom
platform-contracts/pom.xml                     # + grpc-stub, grpc-protobuf; plugin protoc-gen-grpc-java
gateway-service/
├── pom.xml                                    # + grpc-netty-shaded; test: grpc-inprocess
├── src/main/resources/application{,-local,-dev}.yml   # + gateway.grpc, push-api, broadcast, limits.pending-ack
└── src/main/java/com/pangate/gateway/
    ├── config/     GatewayProperties (+Grpc, PushApi, Broadcast, Limits.pendingAck), GatewayLimits (+pending*, broadcastRatePerLoop),
    │               PushConfig (bean mới cho downstream + gRPC + kick)
    ├── server/     NodeState (+shedLevel)
    ├── loop/       LoopContext (+topics(), pacer(), pendingInflight)
    ├── session/    PendingAckWindow, PendingEntry (mới); Session (+pending, topics, overflow), SessionTracker (cleanup)
    ├── downstream/ StreamRef, AckBatcher, PreEncodedFrame, PushDelivery, TopicIndex, TopicPatterns, BroadcastPacer,
    │               ControlPushHandler, LoopBatchTask, PushDispatcher, PushStreamService, PushVersionInterceptor,
    │               SpiffeAuthInterceptor, GrpcServerLifecycle
    ├── registry/   KickClient, PushApiKickClient, SingleSessionKicker (mới); RegistryWriter (single-session)
    ├── pipeline/   SessionHandler (ACK, SUBSCRIBE, UNSUBSCRIBE), PipelineDeps
    └── protocol/   Frames (+error có msgId)
gateway-service/src/it/java/com/pangate/gateway/
    ├── downstream/ GrpcServerLifecycleIT (mTLS, version header), TestPki
    └── it/         GatewayPushIT
gateway-devtools/src/main/java/com/pangate/devtools/
    ├── router/     FakeRouter (thư viện), FakePushApi (thư viện)
    ├── cmd/        FakeRouterCommand, FakePushApiCommand, SmokeCommand (+push→ack), ClientCommand (in PUSH, tự ACK), HealthCommand (+--grpc)
    └── client/     GatewayClient (+awaitPush, ack, subscribe)
platform-devenv/docker-compose.yml             # + fake-pushapi; gateway dev → push-api
Makefile, .github/workflows/ci.yml, README.md, gateway-service/README.md, platform-devenv/README.md
```

Bean mới đăng ký trong `config/PushConfig.java`. `ClusterConfig` chỉ sửa ở bean `RegistryWriter`. `GatewayConfig` chỉ sửa ở bean `PipelineDeps` và `SessionTracker`.

---

### Task 1: Contracts — gRPC stub cho `PushStream` và `PushService`

**Files:**
- Modify: `platform-parent/pom.xml`, `platform-contracts/pom.xml`
- Test: `platform-contracts/src/test/java/com/pangate/contracts/v1/GrpcContractsTest.java`

**Interfaces:**
- Produces:
  - `com.pangate.contracts.v1.PushStreamGrpc`: `PushStreamImplBase`, `newStub(Channel)`, `getStreamMethod()` (BIDI_STREAMING).
  - `com.pangate.contracts.v1.PushServiceGrpc`: `PushServiceImplBase`, `newStub`, `getSendStreamMethod()` (BIDI_STREAMING).
  - Contracts jar mang theo dependency compile `grpc-stub` và `grpc-protobuf`.

- [ ] **Step 1: Viết test thất bại**

```java
class GrpcContractsTest {
  @Test void pushStreamIsBidiStreaming() {
    assertThat(PushStreamGrpc.getStreamMethod().getFullMethodName()).isEqualTo("pangate.v1.PushStream/Stream");
    assertThat(PushStreamGrpc.getStreamMethod().getType()).isEqualTo(MethodDescriptor.MethodType.BIDI_STREAMING);
  }
  @Test void pushServiceIsBidiStreaming() {
    assertThat(PushServiceGrpc.getSendStreamMethod().getFullMethodName()).isEqualTo("pangate.v1.PushService/SendStream");
    assertThat(PushServiceGrpc.getSendStreamMethod().getType()).isEqualTo(MethodDescriptor.MethodType.BIDI_STREAMING);
  }
}
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f platform-contracts test -Dtest=GrpcContractsTest`
Expected: lỗi compile (chưa có `PushStreamGrpc`).

- [ ] **Step 3: Thêm gRPC vào parent và contracts**

Chi tiết:
- `platform-parent`: `<grpc.version>1.76.0</grpc.version>`; import BOM `io.grpc:grpc-bom` (`type=pom`, `scope=import`).
- `platform-contracts`: dependency `io.grpc:grpc-stub` và `io.grpc:grpc-protobuf`. Trong plugin ascopes, thêm binary Maven plugin `io.grpc:protoc-gen-grpc-java:${grpc.version}` với option `@generated=omit`, để code sinh ra không cần `javax.annotation.Generated`.
- Cú pháp plugin của ascopes 5.x: xem `./mvnw -f platform-contracts help:describe -Dplugin=io.github.ascopes:protobuf-maven-plugin -Ddetail`.
- `buf lint` vẫn xanh (proto không đổi).

- [ ] **Step 4: Chạy test**

Run: `./mvnw -q -f platform-parent install && ./mvnw -q -f platform-contracts install`
Expected: PASS. `make buf-lint` (nếu có) hoặc bước buf trong CI không đổi.

- [ ] **Step 5: Commit**

```bash
git add platform-parent/pom.xml platform-contracts
git commit -m "build(contracts): generate gRPC stubs for PushStream and PushService"
```

---

### Task 2: Nền downstream — cấu hình, `NodeState.shedLevel`, `StreamRef`, `AckBatcher`

**Files:**
- Modify:
  - `gateway-service/pom.xml`: thêm `io.grpc:grpc-netty-shaded`; test scope `io.grpc:grpc-inprocess` và `io.grpc:grpc-testing`.
  - `config/GatewayProperties.java`, `config/GatewayLimits.java`, `server/NodeState.java`, `application{,-local,-dev}.yml`.
- Create: `downstream/StreamRef.java`, `downstream/AckBatcher.java`
- Test: `config/GatewayPropertiesTest.java` (bổ sung), `downstream/AckBatcherTest.java`

**Interfaces:**
- Produces:
  - `GatewayProperties`:
    - `+ Grpc grpc`: `record Grpc(int port, boolean plaintext, List<String> allowedSpiffe, GrpcTls tls)`, với `record GrpcTls(Path certFile, Path keyFile, Path trustFile)`.
    - `+ PushApi pushApi`: `record PushApi(boolean enabled, String target, boolean plaintext)`.
    - `+ Broadcast broadcast`: `record Broadcast(int ratePerLoop)`.
    - `Limits` thêm `PendingAck pendingAck`: `record PendingAck(int msgs, DataSize bytes, Duration timeout)`.
  - `GatewayLimits` thêm `int pendingAckMsgs, int pendingAckBytes, Duration pendingAckTimeout, int broadcastRatePerLoop`.
  - `NodeState`: `int shedLevel()` và `void shedLevel(int)` (volatile, mặc định 0; M3 sẽ ghi vào).
  - `StreamRef`:
    ```java
    public final class StreamRef {
      public StreamRef(AckBatcher acks);
      public boolean alive();
      public void markDead();
      public void ack(long deliveryId, AckItem.Status status);  // no-op khi !alive
    }
    ```
  - `AckBatcher`:
    ```java
    public final class AckBatcher {
      public AckBatcher(ServerCallStreamObserver<DeliveryAck> out, ScheduledExecutorService exec, GatewayMetrics m);
      public void offer(AckItem item);   // mọi thread; queue ≥ 256 → submit flush ngay
      public void start();               // lịch flush 2 ms; out.setOnReadyHandler(this::flush)
      public void stop();                // hủy lịch, bỏ phần còn lại
      void flush();                      // chỉ chạy trên exec; gom tối đa 256 item/DeliveryAck; dừng khi !out.isReady()
    }
    ```
  - Config mặc định (`application.yml`):
    - `gateway.grpc: { port: 9090, plaintext: false, allowed-spiffe: [spiffe://pangate/cell-1/push-router], tls: {cert-file: /etc/pangate/grpc/gateway.crt, key-file: /etc/pangate/grpc/gateway.key, trust-file: /etc/pangate/grpc/spiffe-bundle.pem} }`
    - `gateway.push-api: { enabled: true, target: "dns:///push-api:9443", plaintext: false }`
    - `gateway.broadcast.rate-per-loop: 10000`
    - `gateway.limits.pending-ack: { msgs: 32, bytes: 64KB, timeout: 5s }`
  - Profile `local`: `grpc.plaintext=true`, `push-api.enabled=false`.
  - Profile `dev`: `grpc.plaintext=true`, `push-api: { enabled: true, target: "dns:///fake-pushapi:9443", plaintext: true }`.

- [ ] **Step 1: Viết test thất bại**

```java
// GatewayPropertiesTest
@Test void bindsPushSettings() {
  /* application.yml mặc định → grpc.port 9090, plaintext false, allowedSpiffe [spiffe://pangate/cell-1/push-router];
     pushApi.enabled true; broadcast.ratePerLoop 10000; limits.pendingAck = (32, 64KB, 5s);
     GatewayLimits.from(p).pendingAckBytes() == 65536 */ }
@Test void localProfileUsesPlaintextAndNoPushApi() { }

// AckBatcherTest — ServerCallStreamObserver giả (ghi lại onNext, isReady điều khiển được), exec = ScheduledThreadPool(1)
@Test void acksAreBatchedEvery2ms() { /* offer 3 → trong 50 ms có đúng 1 DeliveryAck chứa 3 item theo thứ tự */ }
@Test void queueOf256FlushesImmediatelyInChunksOf256() { /* offer 600 → các DeliveryAck có kích thước 256,256,88 */ }
@Test void notReadyHoldsAcksUntilOnReady() { /* isReady=false → không onNext; bật ready + chạy onReadyHandler → gửi hết */ }
@Test void deadStreamDropsAcksSilently() { /* ref.markDead(); ref.ack(1, ACKED) → không offer, không exception */ }
@Test void countsAcksByStatus() { /* gw.delivery.acks{status=acked} == 2, {status=nack_timeout} == 1 */ }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service test -Dtest='GatewayPropertiesTest,AckBatcherTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực**

Chi tiết:
- Tag `status` của metric là tên enum bỏ tiền tố `STATUS_`, viết thường.
- Chỉ `flush()` được gọi `onNext`, nên luôn có tối đa một flush chạy tại một thời điểm. Bảo đảm bằng cờ `AtomicBoolean flushing`.

- [ ] **Step 4: Chạy test**

Run: lệnh ở Step 2. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): push configuration, shed level and per-stream ack batching"
```

---

### Task 3: Unicast trên owner loop — `PendingAckWindow`, `PushDelivery`, ACK

**Files:**
- Create: `session/PendingEntry.java`, `session/PendingAckWindow.java`, `downstream/PushDelivery.java`
- Modify:
  - `session/Session.java`: thêm `pending` (lazy), bộ đếm overflow, deadline pending, `failPending`.
  - `session/SessionTracker.java`: `onClosed` gọi `s.failPending(...)`.
  - `loop/LoopContext.java`: thêm `pendingInflight` để tính gauge.
  - `pipeline/SessionHandler.java`: xử lý `FRAME_TYPE_ACK`.
- Test: `session/PendingAckWindowTest.java`, `downstream/PushDeliveryTest.java`, `pipeline/SessionHandlerTest.java` (sửa `ackIsIgnored`)

**Interfaces:**
- Consumes: `StreamRef` (Task 2), `GatewayLimits.pendingAck*`, `TenantPolicy.downgradeReliable()`.
- Produces:
  - `record PendingEntry(String msgId, long deliveryId, StreamRef stream, long sentAtSec, int bytes)`
  - `PendingAckWindow`:
    ```java
    public final class PendingAckWindow {           // mảng vòng, n ≤ maxMsgs, quét tuyến tính theo msgId
      public PendingAckWindow(int maxMsgs, int maxBytes);
      public boolean fits(int bytes);               // còn chỗ theo số msg và byte
      public PendingEntry find(String msgId);
      public void add(PendingEntry e);
      public PendingEntry remove(String msgId);     // null nếu không có
      public void replace(String msgId, long deliveryId, StreamRef s);   // router gửi lại
      public long oldestSentAtSec();                // Long.MAX_VALUE nếu rỗng
      public void expire(long nowSec, long timeoutSec, Consumer<PendingEntry> out);
      public void drain(Consumer<PendingEntry> out);
      public int size();
    }
    ```
  - `Session` thêm các method sau (đều chạy trên loop; `pending == null` khi rỗng):
    - `PendingAckWindow pending()` và `PendingAckWindow pendingOrCreate(GatewayLimits l)`. Window rỗng sau ack, timeout hay drain thì đặt lại `null` (`releasePendingIfEmpty()`).
    - `boolean countOverflowNack(long nowSec)`: trả `true` khi đạt ≥ 64 trong cửa sổ 30 s hiện tại. Dùng hai field: `int overflowNacks` và `long overflowWindowSec`.
    - `void failPending(AckItem.Status st)`: ack mọi entry còn lại với `st`.
    - `nextDeadlineSec()` thêm điều kiện `pending.oldestSentAtSec() + pendingAckTimeout` (cho ACTIVE và DRAINING).
    - `onDeadline`: **trước** khi xét close, gọi `pending.expire(now, 5, e -> e.stream().ack(e.deliveryId(), NACK_TIMEOUT))`. Nếu sau đó chưa đến hạn close nào thì `reschedule()`.
  - `PushDelivery`:
    ```java
    public final class PushDelivery {
      public PushDelivery(TenantLimitsStore tenants, LimitsHolder limits, GatewayMetrics m, LongSupplier wallMillis);
      /** Trên owner loop. Trả channel đã ghi (để flush cuối task) hoặc null. */
      public Channel deliver(LoopContext loop, PushItem item, StreamRef stream);
      /** ACK{msg_ids} từ client, trên loop. */
      public void onAck(Session s, Ack ack);
    }
    ```
  - Cách `deliver` xử lý (spec §5.3):
    - Tra session bằng `loop.session(ConnId.sessionKey(connId))`. Session phải tồn tại, `connId.equals(item.connId)` và state ACTIVE hoặc DRAINING. Ngược lại: RELIABLE/ORDERED → `NACK_NOT_FOUND`; EPHEMERAL → drop `reason=not_found`.
    - Ghi bằng `channel.write(item.getFrame(), voidPromise())`. Handoff: nếu `enqueued_at_ms > 0`, ghi `gw.push.handoff` = `wallMillis − enqueued_at_ms`.
    - EPHEMERAL: channel unwritable → drop `reason=unwritable`; ngược lại ghi, không gửi ack.
    - `downgradeReliable` của tenant đang bật → xử lý như EPHEMERAL, rồi ack `STATUS_WRITTEN` (nếu bị drop vì unwritable thì vẫn trả `WRITTEN`).
    - RELIABLE/ORDERED:
      - `msgId` đã có trong window → `replace(...)` để entry trỏ tới `deliveryId` và stream mới. Không ghi lại tới client.
      - Window không còn chỗ → `NACK_OVERFLOW`. Nếu `countOverflowNack` trả `true` → `close(SLOW_CONSUMER, "PENDING_OVERFLOW")`.
      - Còn lại: `add(new PendingEntry(msgId, deliveryId, stream, loop.nowSec(), frame.getSerializedSize()))`, ghi frame, `refreshDeadline()` nếu window vừa từ rỗng thành có entry.
    - `RPC_REQ` không cần nhánh riêng: frame chỉ là frame, xử lý theo class của nó (spec §5.8).
  - `onAck`:
    - Quá 32 id → `throw new ProtocolException(BAD_FRAME, "too many ack ids")`.
    - Mỗi id tìm thấy: `remove`, rồi `stream.ack(deliveryId, ACKED)`. Id lạ hoặc trùng thì bỏ qua.
  - `SessionTracker.onClosed`: `s.failPending(s.closeCode() == SLOW_CONSUMER ? NACK_SLOW_CONSUMER : NACK_CLOSED)`.
  - `SessionHandler`: `FRAME_TYPE_ACK` → `Ack.parseFrom` (lỗi parse → `BAD_FRAME`) → `delivery.onAck`. Constructor nhận thêm `PushDelivery delivery`; `PipelineDeps` thêm field này.

- [ ] **Step 1: Viết test thất bại**

```java
// PendingAckWindowTest
@Test void fitsByCountAndBytes() { /* (2, 100): add 60 B → fits(40) true, fits(41) false; add 40 → fits(1) false (count) */ }
@Test void removeKeepsRingOrder() { /* add a,b,c; remove b; oldestSentAtSec == a; drain → a,c */ }
@Test void expireTakesOnlyOverdue() { /* a@10, b@13; expire(15, 5) → a; expire(18,5) → b */ }

// PushDeliveryTest — LoopFixture + session ACTIVE (connId thật), StreamRef với AckBatcher giả ghi lại item
@Test void reliablePushIsWrittenAndAckedOnClientAck() { /* PUSH tới client; onAck{msgId} → ACKED(deliveryId) */ }
@Test void ephemeralIsWrittenWithoutAck() { }
@Test void ephemeralToUnwritableChannelIsDropped() {
  /* channel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(1, 2)); write 1 KB rác, không flush → !isWritable;
     deliver EPHEMERAL → không ghi thêm; gw.push.dropped{reason=unwritable} == 1 */ }
@Test void wrongConnIdForSameKeyIsNotFound() { /* connId cùng sessionKey nhưng khác rand24 → NACK_NOT_FOUND */ }
@Test void closedSessionReliableNotFoundEphemeralDropped() { }
@Test void duplicateMsgIdRetargetsEntryWithoutRewrite() { /* deliver d1, rồi d2 cùng msgId → 1 frame ra; ACK → chỉ ACKED(d2) */ }
@Test void overflowNacksAndClosesAfter64In30s() {
  /* 32 RELIABLE đầy window; 64 cái tiếp theo → 64 NACK_OVERFLOW, session đóng 4008; các entry còn lại → NACK_SLOW_CONSUMER */ }
@Test void byteLimitAlsoOverflows() { /* payload 40 KB ×2 → cái thứ 2 NACK_OVERFLOW */ }
@Test void downgradeReliableActsEphemeralAndAnswersWritten() { }
@Test void pendingTimesOutAfter5sWithoutClosing() {
  /* deliver; clockSec += 5; loop.advance → NACK_TIMEOUT; session vẫn ACTIVE; pending() == null */ }
@Test void ackAfterTimeoutSendsNothing() { }
@Test void unknownAndDuplicateAckIdsAreIgnored() { }
@Test void closeNacksRemainingPendingAsClosed() { /* channel.close() → NACK_CLOSED */ }
@Test void handoffRecordedWhenEnqueuedAtSet() { }

// SessionHandlerTest (thay ackIsIgnored)
@Test void ackFrameIsRoutedToDelivery() { }
@Test void ackWithMoreThan32IdsClosesBadFrame() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service test -Dtest='PendingAckWindowTest,PushDeliveryTest,SessionHandlerTest,SessionTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực theo Interfaces**

Gauge `gw.pending.ack.inflight` = tổng `LoopContext.pendingInflight()` của mọi loop. Mỗi loop giữ một field `int`, tăng/giảm trên loop; `gw-tick` đọc bằng `VarHandle.getOpaque` (spec §3.3).

- [ ] **Step 4: Chạy test + toàn bộ unit**

Run: `./mvnw -q -f gateway-service test`
Expected: PASS (kể cả `SessionTest` và test deadline của M1).

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): unicast delivery with pending-ack window, client ACK, timeouts and close NACKs"
```

---

### Task 4: `TopicIndex` và SUBSCRIBE / UNSUBSCRIBE từ client

**Files:**
- Create: `downstream/TopicIndex.java`, `downstream/TopicPatterns.java`
- Modify:
  - `loop/LoopContext.java`: thêm `TopicIndex topics()`.
  - `session/Session.java`: thêm `String[] topics` (lazy) cùng `topics()`, `addTopic`, `removeTopic`.
  - `session/SessionTracker.java`: `onClosed` gọi `loop.topics().removeAll(s)`.
  - `pipeline/SessionHandler.java`, `protocol/Frames.java`.
- Test: `downstream/TopicIndexTest.java`, `downstream/TopicPatternsTest.java`, `pipeline/SessionHandlerTest.java`

**Interfaces:**
- Produces:
  - `TopicIndex` (một instance mỗi loop, chỉ dùng trên loop):
    ```java
    public final class TopicIndex {
      public boolean add(Session s, String topic);       // false nếu đã có; cập nhật cả session.topics
      public boolean remove(Session s, String topic);
      public void removeAll(Session s);
      public List<Session> snapshot(String topic);       // bản copy; List.of() nếu không có
      public int topicCount();
    }
    ```
  - `TopicPatterns`:
    - `static boolean inTenant(String tenantId, String topic)`: `topic.startsWith(tenantId + ":") && topic.length() > tenantId.length() + 1`.
    - `static boolean allowed(List<String> patterns, String topic)`: glob, với `*` khớp mọi chuỗi.
  - `Frames.error(int code, String message, boolean retryable, boolean fatal, long retryAfterMs, String msgId)`: ERROR mang `msg_id` của frame gây lỗi.
  - Cách `SessionHandler` xử lý `SUBSCRIBE` / `UNSUBSCRIBE` (payload `Subscribe`):
    - Kiểm toàn bộ trước, rồi áp dụng **tất cả hoặc không gì**.
    - Topic sai tenant hoặc không khớp pattern → `ERROR{4000, "topic not allowed: <topic>", retryable=false, fatal=false, msg_id}`, metric `gw.subscribe.rejected{reason=not_allowed}`.
    - Tổng sau khi thêm vượt `tenants.policy(tenant).topicsPerConn()` → `ERROR{4000, "too many topics", …}`, metric `reason=limit`.
    - Thành công thì không trả lời gì. UNSUBSCRIBE topic chưa đăng ký → bỏ qua.
  - `FrameCodec` đã nhận `SUBSCRIBE` / `UNSUBSCRIBE` inbound, không phải sửa.

- [ ] **Step 1: Viết test thất bại**

```java
// TopicPatternsTest
@Test void globStarMatchesAnyRunIncludingColon() {
  assertThat(TopicPatterns.allowed(List.of("t1:news:*"), "t1:news:vn:hn")).isTrue();
  assertThat(TopicPatterns.allowed(List.of("t1:*:alerts"), "t1:a:b:alerts")).isTrue();
  assertThat(TopicPatterns.allowed(List.of("t1:news"), "t1:news2")).isFalse();
  assertThat(TopicPatterns.allowed(List.of(), "t1:x")).isFalse(); }
@Test void tenantPrefixRequired() {
  assertThat(TopicPatterns.inTenant("t1", "t1:x")).isTrue();
  assertThat(TopicPatterns.inTenant("t1", "t10:x")).isFalse();
  assertThat(TopicPatterns.inTenant("t1", "t1:")).isFalse(); }

// TopicIndexTest
@Test void addRemoveAndSnapshotIsACopy() { }
@Test void removeAllClearsEveryTopicOfSession() { /* topic rỗng bị xóa khỏi map: topicCount() == 0 */ }

// SessionHandlerTest
@Test void subscribeAllowedTopicsIndexesThem() { /* patterns ["t1:news:*"]; SUBSCRIBE t1:news:a,t1:news:b → loop.topics().snapshot(..) chứa session */ }
@Test void subscribeWithOneForbiddenTopicAppliesNoneAndErrors4000WithMsgId() { }
@Test void subscribeBeyondTopicsPerConnErrors() { /* TenantCfg topics_per_conn=2 → topic thứ 3 bị từ chối */ }
@Test void unsubscribeRemovesAndUnknownIsIgnored() { }
@Test void closeRemovesSessionFromTopicIndex() { }
```

Cập nhật test `unsupportedFramesGetNonFatalError` của M1: không còn frame client nào là UNSUPPORTED. Xóa test đó và thay bằng các test ở trên.

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service test -Dtest='TopicPatternsTest,TopicIndexTest,SessionHandlerTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực**

Chi tiết:
- `TopicIndex` dùng `Object2ObjectOpenHashMap<String, ObjectArrayList<Session>>`.
- `session.topics` là `String[]`, `null` khi không có topic. Mảng tăng kích thước khi thêm, tối đa `topicsPerConn`.

- [ ] **Step 4: Chạy test**

Run: `./mvnw -q -f gateway-service test`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): per-loop topic index with client SUBSCRIBE/UNSUBSCRIBE and pattern checks"
```

---

### Task 5: `PreEncodedFrame` và `BroadcastPacer`

**Files:**
- Create: `downstream/PreEncodedFrame.java`, `downstream/BroadcastPacer.java`
- Modify: `loop/LoopContext.java` (`BroadcastPacer pacer()` + setter `pacer(BroadcastPacer)`; `PushConfig` gắn pacer cho mỗi loop lúc wiring. `TopicIndex` đã là field `final` tạo trong constructor của `LoopContext` từ Task 4)
- Test: `downstream/PreEncodedFrameTest.java`, `downstream/BroadcastPacerTest.java`

**Interfaces:**
- Consumes: `TopicIndex.snapshot` (Task 4), `NodeState.shedLevel`, `TenantPolicy.broadcastEnabled()`.
- Produces:
  - `PreEncodedFrame`:
    ```java
    public final class PreEncodedFrame {
      /** Encode protobuf một lần vào direct ByteBuf (PooledByteBufAllocator.DEFAULT), refCnt = shares. */
      public static PreEncodedFrame encode(Frame f, int shares);
      public ByteBuf retainedDuplicate();
      public void releaseShare();            // gọi đúng một lần mỗi loop
      public DeliveryClass deliveryClass();
      public String tenantId();              // của PushItem, để kiểm kill switch
    }
    ```
    `ByteBuf` đi qua `FrameCodec` (codec chỉ encode `Frame`) tới `WsBinaryAdapter` (WS) hoặc `LengthFieldPrepender` (TCP). Không phải sửa pipeline.
  - `BroadcastPacer` (mỗi loop một cái, chạy trên loop):
    ```java
    public final class BroadcastPacer {
      public BroadcastPacer(LoopContext loop, NodeState node, TenantLimitsStore tenants, LimitsHolder limits, GatewayMetrics m);
      /** Trong LoopBatchTask. Luôn nhận đúng một share của frame và tự release. */
      public void submit(String topic, PushItem.Priority p, PreEncodedFrame frame);
      public int queued();
    }
    ```
  - Cách `submit` xử lý:
    1. `broadcastEnabled=false` (tenant của frame) và priority ≠ CRITICAL → drop, `releaseShare()`, metric `gw.push.dropped{reason=broadcast_disabled}`.
    2. Lấy `snapshot(topic)`. Danh sách rỗng → `releaseShare()`.
    3. CRITICAL → ghi toàn bộ ngay, flush, release.
    4. NORMAL hoặc BULK → xếp vào hàng đợi tương ứng. Nếu chưa có lịch thì `schedule(tick, 10 ms)`.
  - `tick()` được ghi tối đa `max(1, broadcastRatePerLoop / 100)` recipient, ưu tiên NORMAL trước. Bỏ qua BULK khi `node.shedLevel() ≥ 1`. Còn job (và không chỉ còn BULK đang bị dừng) thì lên lịch lại.
  - Trong lúc đang shed, pacer kiểm lại mỗi 100 ms (không phải 10 ms).
  - Mỗi recipient:
    - State khác ACTIVE/DRAINING hoặc `!channel.isWritable()` → bỏ qua (spec §6.4). Áp dụng cho cả frame RELIABLE có `topic_seq`.
    - Ngược lại: `write(retainedDuplicate(), voidPromise())`. Flush các channel đã ghi ở cuối lượt.
  - Metric `gw.broadcast.recipients{priority}`.

- [ ] **Step 1: Viết test thất bại**

```java
// PreEncodedFrameTest
@Test void encodesOnceAndDuplicatesShareContent() { /* decode retainedDuplicate == frame; refCnt giảm về 0 sau shares lần releaseShare */ }

// BroadcastPacerTest — LoopFixture với nhiều EmbeddedChannel/Session cùng loop (helper fixture.activeSession(i)),
//   ResourceLeakDetector PARANOID
@Test void criticalWritesAllRecipientsImmediately() { }
@Test void normalIsPacedPerTick() { /* ratePerLoop 1000 → 10 recipient mỗi tick: 25 subscriber → 10,10,5 qua runScheduledPendingTasks + tiến clock 10 ms */ }
@Test void normalBeforeBulk() { }
@Test void bulkPausedWhileShedding() { /* shedLevel 1 → BULK không ghi; về 0 → ghi tiếp */ }
@Test void broadcastDisabledDropsNonCritical() { }
@Test void unwritableRecipientSkipped() { }
@Test void closedRecipientIsSkippedAndBufferReleased() { /* đóng 1 session giữa hai tick; cuối cùng frame.refCnt()==0 */ }
@Test void loopWithoutSubscribersReleasesItsShare() { }
@Test void noTimerWhenIdle() { /* sau khi xong job: không còn scheduled task của pacer */ }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service test -Dtest='PreEncodedFrameTest,BroadcastPacerTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực theo Interfaces**

Snapshot danh sách recipient được chụp lúc nhận job (spec §6.4). Job lưu `frame`, `Session[]` và con trỏ vị trí.

- [ ] **Step 4: Chạy test**

Run: lệnh ở Step 2. Expected: PASS, không có log `LEAK:`.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): pre-encoded broadcast frames and per-loop broadcast pacer"
```

---

### Task 6: `ControlPushHandler` — subscribe, unsubscribe, kick, refresh

**Files:**
- Create: `downstream/ControlPushHandler.java`
- Test: `downstream/ControlPushHandlerTest.java`

**Interfaces:**
- Consumes: `PrincipalIndex`, `TopicIndex`, `TopicPatterns.inTenant`, `TenantLimitsStore`.
- Produces:
  ```java
  public final class ControlPushHandler {
    public ControlPushHandler(PrincipalIndex principals, TenantLimitsStore tenants, GatewayMetrics m);
    /** Trên loop: áp dụng op cho mọi session của principal thuộc loop này (spec §6.5). */
    public void apply(LoopContext loop, String principal, ControlOp op);
    /** Dùng chung cho KICK từ router và kick local của single-session (Task 10). */
    public void kick(LoopContext loop, String principal, String newConnId, long connectedAtMs);
  }
  ```
- Cách từng op xử lý:
  - `SUBSCRIBE`: thêm các topic qua `inTenant` của tenant session (không kiểm pattern), dừng khi đạt `topicsPerConn`. Topic bị loại được đếm `gw.subscribe.rejected{reason=control}`.
  - `UNSUBSCRIBE`: gỡ topic.
  - `KICK`: close `DUPLICATE_SESSION` (4012, reason `"KICKED"`) cho session có `!connId.equals(newConnId) && connectedAtMs() < connectedAtMs`. Lifecycle `KICKED` đã được map từ 4012 (M2a).
  - `REFRESH_CONFIG`: không có cache theo principal để xóa, chỉ đếm metric.
  - Metric `gw.control.push{op=subscribe|unsubscribe|kick|refresh_config}`.
  - Session được tìm qua `principals.get(principal)` lọc theo `ref.loopIdx() == loop.index()`, rồi `loop.session(key)`; kiểm state ACTIVE/DRAINING và `principalId` khớp.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void controlSubscribeAddsOnlyTenantTopicsIgnoringPatterns() { }
@Test void controlSubscribeStopsAtTopicsPerConn() { }
@Test void controlUnsubscribeRemoves() { }
@Test void kickClosesOlderSessions4012() { }
@Test void kickSparesNewConnAndNewerSessions() {
  /* 3 session cùng principal: A (cũ), B (= newConnId), C (connectedAt > kick.connected_at_ms) → chỉ A đóng 4012 */ }
@Test void otherPrincipalsAndLoopsUntouched() { }
@Test void refreshConfigCountsOnly() { }
```

- [ ] **Step 2–4:** Chạy `./mvnw -q -f gateway-service test -Dtest=ControlPushHandlerTest`: FAIL (compile), hiện thực, rồi PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): control push ops (subscribe, unsubscribe, kick, refresh) on owner loops"
```

---

### Task 7: `PushDispatcher` và `LoopBatchTask`

**Files:**
- Create: `downstream/PushDispatcher.java`, `downstream/LoopBatchTask.java`
- Test: `downstream/PushDispatcherTest.java`

**Interfaces:**
- Consumes: `PushDelivery` (Task 3), `BroadcastPacer` (Task 5), `ControlPushHandler` (Task 6), `PreEncodedFrame`, `ConnId`, `PrincipalIndex`, `NodeState.shedLevel`.
- Produces:
  ```java
  public final class PushDispatcher {
    public PushDispatcher(LoopRegistry loops, PrincipalIndex principals, NodeState node, PushDelivery delivery,
        ControlPushHandler control, GatewayMetrics m);
    /** Trên executor gRPC. Mỗi loop có item nhận đúng một execute(LoopBatchTask). */
    public void dispatch(PushBatch batch, StreamRef stream);
  }
  final class LoopBatchTask implements Runnable { /* items của một loop; chạy delivery/pacer/control rồi flush các channel đã ghi */ }
  ```
- Cách `dispatch` chia item:
  - `KIND_MESSAGE`:
    - `node.shedLevel() ≥ 4` → RELIABLE/ORDERED nhận `NACK_SHED` (ack thẳng qua `stream`), EPHEMERAL bị drop `reason=shed`.
    - `!ConnId.isValid(connId)` hoặc `loopIdx ≥ loops.size()` → RELIABLE/ORDERED nhận `NACK_NOT_FOUND`, EPHEMERAL bị drop `reason=not_found`.
    - Còn lại: đưa vào bucket của `ConnId.loopIdx`.
  - `KIND_BROADCAST`: `PreEncodedFrame.encode(frame, loops.size())` rồi thêm vào bucket của **mọi** loop. Topic rỗng → bỏ qua và đếm `reason=bad_item`.
  - `KIND_CONTROL`: lấy tập loop từ `principals.get(principal)`. Principal không có session trên node → bỏ qua.
  - `KIND_UNSPECIFIED` → đếm `gw.push.dropped{reason=bad_item}`. Nếu frame là RELIABLE thì trả `NACK_NOT_FOUND` để giữ contract "mỗi delivery_id đúng một ack".
- `LoopBatchTask.run()` (trên loop): gom các `Channel` đã ghi vào `ReferenceOpenHashSet`, gọi `flush()` cho từng cái ở cuối task.

- [ ] **Step 1: Viết test thất bại**

```java
// LoopRegistry.fromContexts với 2 LoopContext trên 2 DefaultEventLoop thật; delivery/pacer/control là fake ghi lại (loopIdx, item)
@Test void messagesBucketedByConnIdLoopOneTaskPerLoop() { /* 3 item loop0, 2 item loop1 → đúng 2 execute */ }
@Test void broadcastEncodedOnceSharedByAllLoops() { /* refCnt ban đầu == 2; mỗi loop nhận một submit */ }
@Test void controlGoesOnlyToLoopsHoldingPrincipal() { }
@Test void messageToUnknownOrMalformedConnIdIsNackedNotFound() { /* "xyz", loopIdx 7 với 2 loop → NACK_NOT_FOUND; EPHEMERAL → drop */ }
@Test void shedLevel4NacksReliableAndDropsEphemeral() { }
@Test void writtenChannelsFlushedOncePerTask() { }
```

- [ ] **Step 2–4:** Chạy `./mvnw -q -f gateway-service test -Dtest=PushDispatcherTest`: FAIL, hiện thực, PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): push dispatcher splitting batches into one task per event loop"
```

---

### Task 8: `PushStreamService`, interceptor và `GrpcServerLifecycle`

**Files:**
- Create: `downstream/PushStreamService.java`, `downstream/PushVersionInterceptor.java`, `downstream/SpiffeAuthInterceptor.java`, `downstream/GrpcServerLifecycle.java`, `config/PushConfig.java`
- Modify:
  - `config/GatewayConfig.java`: thêm `PipelineDeps(+PushDelivery)`; `SessionTracker` không đổi chữ ký.
- Test: `downstream/PushStreamServiceTest.java` (in-process), `downstream/SpiffeAuthInterceptorTest.java`, `src/it/java/.../downstream/GrpcServerLifecycleIT.java`, `src/it/java/.../downstream/TestPki.java`

**Interfaces:**
- Consumes: `PushDispatcher`, `AckBatcher`, `StreamRef`, `NodeState`, `GatewayProperties.grpc`.
- Produces:
  - `PushStreamService extends PushStreamGrpc.PushStreamImplBase`. `stream(StreamObserver<DeliveryAck>)` làm các việc sau:
    - Ép kiểu sang `ServerCallStreamObserver`, gọi `disableAutoRequest()` rồi `request(8)`.
    - Tạo `AckBatcher` + `StreamRef`, `start()`.
    - `onNext(batch)`: `dispatcher.dispatch(batch, ref)`, sau đó `request(1)` nếu `shedLevel < 4`; ngược lại thêm stream vào tập `paused`.
    - `onError` / `onCompleted` / `setOnCancelHandler`: `ref.markDead()`, `batcher.stop()`, gỡ khỏi tập active. Riêng `onCompleted` còn gọi `responseObserver.onCompleted()`.
    - `void resumePaused()`: `gw-tick` gọi mỗi 100 ms; khi `shedLevel < 4` thì `request(1)` cho các stream `paused`.
    - Gauge `gw.pushstream.active`.
  - `PushVersionInterceptor implements ServerInterceptor`: header `pangate-push-version` (`Metadata.Key<String>`) phải thuộc `{"1"}` (tính theo Global Constraints). Sai → `call.close(Status.FAILED_PRECONDITION.withDescription("unsupported pangate-push-version"), new Metadata())`.
  - `SpiffeAuthInterceptor(List<String> allowed) implements ServerInterceptor`:
    - Đọc `call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION)` rồi lấy cert peer đầu tiên.
    - URI SAN (type 6) phải nằm trong `allowed`. Thiếu session hoặc SAN, hoặc SAN không khớp → `UNAUTHENTICATED`.
    - Chỉ gắn vào server khi `!plaintext`.
  - `GrpcServerLifecycle implements SmartLifecycle`:
    - Phase `Integer.MAX_VALUE − 1700`.
    - `start()`: `NettyServerBuilder.forPort(port)`, `bossEventLoopGroup` và `workerEventLoopGroup` dùng chung một shaded `NioEventLoopGroup(2, "gw-grpc-el")`, `channelType(NioServerSocketChannel.class)` (shaded), `executor(gw-grpc-exec ×2)`, `intercept(version, spiffe?)`. TLS lấy từ `GrpcSslContexts.forServer(cert, key).trustManager(trustFile).clientAuth(REQUIRE)` khi `!plaintext`.
    - Port bị chiếm → `ListenerBindException`, dùng lại analyzer của M1 với biến môi trường gợi ý `GATEWAY_GRPC_PORT`.
    - `stop()`: `shutdown()` → `awaitTermination(5 s)` → `shutdownNow()`, rồi đóng event loop group và executor.
    - `int port()`: port thật (để test dùng port 0).
  - `PushConfig` khai báo bean cho: `PushDelivery`, `ControlPushHandler`, `PushDispatcher`, `PushStreamService` (cùng tick `resumePaused` 100 ms), `GrpcServerLifecycle`, các gauge, và gọi `loop.pacer(new BroadcastPacer(...))` cho mỗi loop.

- [ ] **Step 1: Viết test thất bại**

```java
// PushStreamServiceTest — InProcessServerBuilder + PushVersionInterceptor; dispatcher fake ghi lại batch
@Test void batchesAreDispatchedAndAcksStreamedBack() { /* fake dispatcher gọi ref.ack(7, ACKED) → client nhận DeliveryAck[7 ACKED] */ }
@Test void initialRequest8ThenOnePerBatch() { /* client gửi 20 batch không chờ: server nhận ≤ 8 trước khi dispatch xử lý xong */ }
@Test void shedLevel4PausesRequestsAndTickResumes() { }
@Test void missingOrWrongVersionHeaderFailsPrecondition() { }
@Test void streamCancelMarksRefDead() { /* client cancel → ref.alive()==false; gauge về 0 */ }

// SpiffeAuthInterceptorTest — ServerCall giả với SSLSession giả trả X509Certificate có SAN URI (tạo bằng TestPki)
@Test void allowedSpiffeIdPasses() { }
@Test void otherSpiffeIdOrNoSslSessionIsUnauthenticated() { }

// GrpcServerLifecycleIT (Testcontainers không cần) — TestPki: CA + server cert (SAN localhost) + 2 client cert (SAN URI)
@Test void mtlsWithAllowedClientCertStreams() { }
@Test void mtlsWithOtherSpiffeIdIsUnauthenticated() { }
@Test void plaintextServesAndStopsWithin5s() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service test -Dtest='PushStreamServiceTest,SpiffeAuthInterceptorTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực theo Interfaces**

- [ ] **Step 4: Chạy test + IT liên quan**

Run: `./mvnw -q -f gateway-service verify -Dit.test=GrpcServerLifecycleIT -Dtest='PushStreamServiceTest,SpiffeAuthInterceptorTest'`
Expected: PASS. `GatewayLocalIT` (chạy trong `verify` đầy đủ ở Task 11) phải chạy được với gRPC trên port ngẫu nhiên: thêm `--gateway.grpc.port=<freePort>` vào helper start của các IT hiện có.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): gRPC PushStream server with flow control, version header and SPIFFE mTLS"
```

---

### Task 9: Single-session — `KickClient` và kick local

**Files:**
- Create: `registry/KickClient.java` (interface), `registry/PushApiKickClient.java`, `registry/SingleSessionKicker.java`
- Modify: `registry/RegistryWriter.java` (single-session theo tenant, callback entry bị thay), `config/ClusterConfig.java`, `config/PushConfig.java`
- Test: `registry/SingleSessionKickerTest.java`, `registry/PushApiKickClientTest.java` (in-process), `src/it/.../registry/RegistryWriterIT.java` (bổ sung)

**Interfaces:**
- Consumes: `ControlPushHandler.kick` (Task 6), `TenantPolicy.singleSession()`, `NodeState`, `RegistryWriter.value` (định dạng `nodeId|epoch|connectedAtMs|platform`).
- Produces:
  - `RegistryWriter`:
    - Constructor thêm `TenantLimitsStore tenants` và `BiConsumer<Session, Map<String,String>> onReplaced`.
    - `activated(s)` gọi `register(s, tenants.policy(tenant).singleSession())`.
    - Mỗi lần register thành công ở chế độ single (kể cả lần retry) thì gọi `onReplaced(s, prev)` **trên loop của session**, sau khi đã bỏ entry `connId` của chính nó khỏi `prev`.
    - `RegistryResync` vẫn gọi `register(s, false)`.
  - `interface KickClient { void kick(String tenant, String principal, String newConnId, long connectedAtMs); }`. Bản no-op dùng khi `push-api.enabled=false`; nó ghi WARN `"single-session entry on node {} not kicked: push-api disabled"`.
  - `SingleSessionKicker implements BiConsumer<Session, Map<String,String>>`. Với mỗi entry `(connId, "nodeId|epoch|…")`:
    - `nodeId` bằng `node.nodeId()` và `epoch` bằng `node.epoch()` → kick local. Dùng `principals.get(p)` để tìm các loop liên quan, rồi `loop.eventLoop().execute(() -> control.kick(loop, p, s.connId(), s.connectedAtMs()))`.
    - Ngược lại → `kickClient.kick(...)`. Gọi **một lần** cho mỗi principal, kể cả khi có nhiều entry ở node khác.
    - Entry có giá trị sai định dạng → bỏ qua, ghi WARN.
  - `PushApiKickClient implements KickClient, AutoCloseable`:
    - Một `ManagedChannel` tới `push-api.target`. Plaintext dùng `usePlaintext()`; ngược lại mTLS với cert `grpc.tls.*`.
    - Một stream `SendStream` dùng lâu dài, mở lười và mở lại khi lỗi.
    - `request_id` là UUID. Các request đang chờ nằm trong `ConcurrentHashMap<String, CompletableFuture<PushResponse>>`.
    - Timeout 2 s mỗi lần thử. Retry sau 100 ms rồi 400 ms, lên lịch trên `TickScheduler`.
    - Thất bại cuối cùng → `gw.kick.failures` + WARN. Không bao giờ chạy trên loop.
    - Đóng channel khi context shutdown (`destroyMethod`).

- [ ] **Step 1: Viết test thất bại**

```java
// SingleSessionKickerTest — LoopFixture, NodeState(nodeId "n1", epoch 5), KickClient fake ghi lại
@Test void localPreviousEntryKickedRemoteSentToPushApi() {
  /* prev {C0: "n1|5|...", C9: "n2|3|..."} → session C0 local đóng 4012; kickClient.kick(t1, p, newConnId, connectedAt) đúng 1 lần */ }
@Test void sameNodeOlderEpochIsRemote() { /* "n1|4|..." → gửi qua KickClient (incarnation cũ không còn session local) */ }
@Test void malformedEntryIgnored() { }
@Test void disabledPushApiOnlyWarns() { }

// PushApiKickClientTest — InProcess PushService giả
@Test void sendsControlKickAndCompletesOnOkResponse() { /* PushRequest kind CONTROL, control.kick.new_conn_id */ }
@Test void retriesTwiceThenCountsFailure() { /* server trả ok=false 3 lần → đúng 3 request, gw.kick.failures == 1 */ }
@Test void reopensStreamAfterError() { }

// RegistryWriterIT (bổ sung)
@Test void singleSessionTenantReplacesEntriesAndReportsPrevious() {
  /* TenantCfg single_session=true cho t1 trong TenantLimitsStore; 2 session cùng principal → onReplaced nhận {connId1: ...}; HGETALL chỉ còn connId2 */ }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service test -Dtest='SingleSessionKickerTest,PushApiKickClientTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực theo Interfaces** (chạy `impact RegistryWriter` trước khi sửa)

- [ ] **Step 4: Chạy test + IT registry**

Run: `./mvnw -q -f gateway-service verify -Dit.test=RegistryWriterIT -Dtest='SingleSessionKickerTest,PushApiKickClientTest,RegistryResyncTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): single-session mode with local kick and Push API KickClient"
```

---

### Task 10: Devtools — `FakeRouter`, `FakePushApi`, client push/ACK/subscribe

**Files:**
- Modify:
  - `gateway-devtools/pom.xml`: thêm `grpc-netty-shaded`, `grpc-services`; test scope `grpc-inprocess`.
  - `Main.java`, `client/GatewayClient.java`, `cmd/ClientCommand.java`, `cmd/HealthCommand.java`.
- Create: `router/FakeRouter.java`, `router/FakePushApi.java`, `cmd/FakeRouterCommand.java`, `cmd/FakePushApiCommand.java`
- Test: `router/FakeRouterTest.java`, `router/FakePushApiTest.java`, `client/GatewayClientTest.java`

**Interfaces:**
- Produces (gateway IT và smoke sẽ dùng như thư viện):
  - `FakeRouter`:
    ```java
    public final class FakeRouter implements AutoCloseable {
      public static FakeRouter connect(String target);            // plaintext, header pangate-push-version: 1
      public long unicast(String connId, DeliveryClass cls, String msgId, byte[] payload);   // trả deliveryId
      public void broadcast(String tenant, String topic, PushItem.Priority p, byte[] payload);
      public void control(String tenant, String principal, ControlOp op);
      public void send(PushBatch batch);
      public AckItem awaitAck(long deliveryId, Duration timeout);  // TimeoutException nếu không có
      public List<AckItem> acks();                                 // tất cả đã nhận
    }
    ```
    `deliveryId` tăng dần từ 1. `enqueued_at_ms` = thời điểm hiện tại. Frame là `PUSH`, kèm `msg_id`, `class` và payload. Broadcast đặt `topic` vào cả item lẫn frame.
  - `FakePushApi`:
    ```java
    public final class FakePushApi implements AutoCloseable {
      public static FakePushApi start(int port, List<String> routerTargets);  // port 0 → ngẫu nhiên
      public int port();
      public List<PushRequest> received();
    }
    ```
    Với mỗi `PushRequest{KIND_CONTROL}`: gửi `PushItem{KIND_CONTROL, tenant, principal, control}` qua `FakeRouter` tới **mọi** `routerTargets`, rồi trả `PushResponse{request_id, ok=true}`. Lỗi → `ok=false, error`. Đăng ký thêm `HealthStatusManager`, status `SERVING`.
  - Lệnh:
    - `fake-router --target host:9090 unicast --conn-id X [--class reliable|ordered|ephemeral] [--payload s] [--wait-ack 10s]`
    - `fake-router … broadcast --tenant t --topic t:x [--priority critical|normal|bulk]`
    - `fake-router … control --tenant t --principal p (--subscribe a,b | --unsubscribe a | --kick-new-conn C --connected-at-ms N)`
    - `fake-router … load --conn-id X --rate R --count N`: in số ack theo status.
    - `fake-pushapi --port 9443 --router gateway:9090[,…]`: chạy tới SIGTERM.
    - `health --grpc host:port`: gọi `grpc.health.v1.Health/Check`, exit 0 nếu SERVING.
  - `GatewayClient`:
    - `Frame awaitPush(Duration)`: frame `PUSH` hoặc `RPC_REQ` kế tiếp.
    - `void ack(String... msgIds)`.
    - `void subscribe(String... topics)` và `void unsubscribe(String... topics)`: gửi frame có `msg_id`.
  - `ClientCommand`: in mỗi PUSH ra stdout (`PUSH msg_id=… class=… bytes=…`). Tự `ack` các frame RELIABLE/ORDERED. Thêm `--subscribe a,b`.

- [ ] **Step 1: Viết test thất bại**

```java
// FakeRouterTest — InProcess/Netty PushStream server giả: trả ACKED cho mọi RELIABLE, kiểm header version
@Test void unicastSendsVersionHeaderAndAwaitsAck() { }
@Test void awaitAckTimesOut() { }
// FakePushApiTest — router giả ghi lại PushBatch
@Test void kickRequestBecomesControlItemOnEveryRouterAndIsAcked() { }
@Test void healthServing() { }
// GatewayClientTest (FakeGatewayServer gửi PUSH)
@Test void awaitPushAndAckSendsAckFrame() { }
@Test void subscribeSendsSubscribePayload() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-devtools test -Dtest='FakeRouterTest,FakePushApiTest,GatewayClientTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực**, rồi `./mvnw -q -f gateway-devtools install`.

- [ ] **Step 4: Chạy test**

Run: lệnh ở Step 2. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway-devtools
git commit -m "feat(devtools): fake-router and fake-pushapi, client push/ack/subscribe, grpc health"
```

---

### Task 11: IT end-to-end M2b

**Files:**
- Create: `gateway-service/src/it/java/com/pangate/gateway/it/GatewayPushIT.java`
- Modify: `GatewayClusterIT.java`, `GatewayLocalIT.java` (start helper thêm `--gateway.grpc.port=<free>`)

**Interfaces:**
- Consumes: `FakeRouter`, `FakePushApi`, `GatewayClient` (Task 10), `InfraContainers`, helper `Gateway.start` (copy từ `GatewayClusterIT`, thêm cổng gRPC).
- Produces:
  - Mỗi class có hai gateway thật, `A` và `B`, cùng Redis và Kafka, `--gateway.io.threads=2`.
  - `B` chạy `--gateway.push-api.enabled=true --gateway.push-api.target=localhost:<fakePushApi> --gateway.push-api.plaintext=true`.
  - `FakePushApi` route tới gRPC của `A`.

- [ ] **Step 1: Viết các kịch bản IT**

```java
@Test void reliablePushAckedEndToEnd() { /* router.unicast(connId, RELIABLE) → client awaitPush → ack → router.awaitAck == ACKED */ }
@Test void unackedPushTimesOutAfter5s() { /* không ack → NACK_TIMEOUT trong 5–7 s; client vẫn PING/PONG */ }
@Test void ephemeralGetsNoAck() { /* awaitPush; router.acks() rỗng sau 1 s */ }
@Test void rpcReqDeliveredAndRpcResGoesUpstreamAsCmdResult() { /* frame RPC_REQ correlation_id; client gửi RPC_RES → record c1.gw.upstream.cmd_result */ }
@Test void pushToClosedConnIsNotFound() { }
@Test void closeWithPendingNacksClosed() { }
@Test void broadcastReachesSubscribersOnEveryLoop() {
  /* mở client tới khi có connId thuộc loop 0 và loop 1 (ConnId.loopIdx); tất cả SUBSCRIBE t1:news; 1 client không subscribe;
     broadcast NORMAL → mọi subscriber nhận đúng 1 PUSH topic t1:news, client kia không nhận; không có ack */ }
@Test void controlSubscribeThenBroadcast() { }
@Test void singleSessionKickSameNode() {
  /* TenantCfg tenant t-ss single_session=true (gw.control) → HELLO c1 rồi c2 cùng principal trên A → c1 close 4012; lifecycle KICKED cho c1 */ }
@Test void singleSessionKickAcrossNodesViaPushApi() {
  /* c1 trên A, c2 cùng principal trên B → FakePushApi nhận KICK → c1 close 4012 trong < 5 s; c2 vẫn mở */ }
@Test void wrongVersionHeaderRejected() { }
```

- [ ] **Step 2: Chạy toàn bộ**

Run: `make bootstrap && ./mvnw -f gateway-service verify`
Expected: mọi unit test và IT xanh, gồm `GatewayLocalIT`, `GatewayClusterIT`, component IT và `GatewayPushIT`.

- [ ] **Step 3: Sửa lỗi lộ ra** trong task sở hữu code đó, rồi chạy lại Step 2.

- [ ] **Step 4: Commit**

```bash
git add gateway-service
git commit -m "test(gateway): M2b end-to-end ITs (push/ack, timeout, broadcast across loops, control, kick across nodes)"
```

---

### Task 12: Smoke push → ack, compose `fake-pushapi`, CI và tiêu chí hoàn thành M2

**Files:**
- Modify:
  - `gateway-devtools/src/main/java/com/pangate/devtools/cmd/SmokeCommand.java` (và test của nó).
  - `platform-devenv/docker-compose.yml`, `Makefile`, `.github/workflows/ci.yml`.
  - `README.md`, `gateway-service/README.md`, `platform-devenv/README.md`.
  - Spec §11 (cập nhật dòng bộ nhớ nếu số đo đổi) và §14 (TODO 50k vẫn còn).
  - Plan này: thêm mục kết quả đo.

**Interfaces:**
- Produces:
  - `SmokeCommand`:
    - Thêm `--router <target>` (bắt buộc).
    - Sau bước upstream: `FakeRouter.connect(router).unicast(first.connId, RELIABLE, msgId, "smoke-push")`, rồi client `awaitPush` (msg_id phải khớp) và `ack`. Router `awaitAck` phải trả `ACKED`.
    - Sau đó đóng kết nối và chạy bước resume như cũ.
    - In ra `SMOKE OK (connected, pong, upstream-ack, push-ack, resume)`.
  - Compose: thêm service `fake-pushapi`:
    - `profiles: ["app"]`, image `pangate/gateway-devtools:dev`.
    - `command: ["fake-pushapi", "--port", "9443", "--router", "gateway:9090"]`.
    - Healthcheck: `java -cp @/app/jib-classpath-file com.pangate.devtools.Main health --grpc localhost:9443`.
    - `gateway` thêm `depends_on: fake-pushapi: service_healthy`. `fake-pushapi` chỉ kết nối tới gateway khi cần gửi KICK, nên thứ tự này không tạo vòng phụ thuộc.
  - `Makefile smoke` thêm `--router gateway:9090`.
  - CI: job `dev-start` không đổi lệnh; khi fail thì in thêm log của `fake-pushapi`.

- [ ] **Step 1: Viết test thất bại cho smoke**

```java
// SmokeCommandTest (nếu có) hoặc kiểm bằng option parsing: thiếu --router → exit 2 với usage
@Test void routerOptionRequired() { }
```

Run: `./mvnw -q -f gateway-devtools test -Dtest=SmokeCommandTest`. Expected: FAIL.

- [ ] **Step 2:** Hiện thực smoke, compose, Makefile, README và CI. Chạy `./mvnw -q -f gateway-devtools install`. Expected: PASS.

- [ ] **Step 3: R1 và R3 ở local**

Run: `make bootstrap && make local-check`
Expected: `LOCAL START OK`. Gateway mở cổng 9090 plaintext; `make doctor` vẫn kiểm cổng 9090 còn trống trước khi start.

- [ ] **Step 4: Heap và tải idle**

Run: `make memory-check && make load-check N=18000`
Expected: heap/session < 5120 B; lag p99 < 20 ms.

- [ ] **Step 5: R2 ở dev cùng smoke push → ack**

Run: `make images && make up && make smoke; make down`
Expected: mọi container (kể cả `fake-pushapi`) healthy trong ≤ 90 s. In ra `SMOKE OK (connected, pong, upstream-ack, push-ack, resume)`.

- [ ] **Step 6: Ghi kết quả**

Thêm vào cuối plan này mục `## Kết quả đo M2b`, gồm các chỉ số:
- Ready local.
- Thời gian dừng.
- Compose tới healthy.
- Heap/session.
- Lag p99 với 18k session.
- Thời gian ACK end-to-end trong smoke.
- Số test (unit + IT).

- [ ] **Step 7: Kiểm thay đổi rồi commit**

```bash
node .gitnexus/run.cjs detect-changes --scope staged --repo .
git add -A
git commit -m "chore: M2b smoke push->ack, fake-pushapi in compose, docs and exit measurements"
```

**Tiêu chí hoàn thành M2** (spec §12, gộp M2a và M2b):
- R1 với Kafka và Redis trên `localhost`; R2 đầy đủ (compose có `fake-pushapi`, smoke đi hết connect → push → ack → disconnect).
- Diễn tập kill node và Revoke < 10 s: đã đạt ở M2a, `GatewayClusterIT` vẫn xanh.
- IT M2 xanh, gồm `GatewayPushIT`.
- Heap/session < 5 KB; lag p99 < 20 ms với 18k session.
- TODO 50k session (spec §14) vẫn mở.
