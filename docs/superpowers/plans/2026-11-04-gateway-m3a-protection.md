# Gateway M3a (Bảo vệ node và vòng đời) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Gateway tự bảo vệ khi quá tải và có vòng đời an toàn. Cụ thể:
- Đo outbound budget.
- Load-shedding ladder level 1–5 có hysteresis, cùng các hành động của từng level.
- Đóng slow consumer.
- Drain theo đợt khi SIGTERM hoặc `/admin/drain`.
- Recycling kết nối.
- Warm-up trước khi ready, sau đó admission ramp.
- Kill switch `admission.ratePerNode` và `recycle.enabled`.
- Metric kernel.

**Architecture:**
- **Tín hiệu:** mọi tín hiệu được lấy mẫu trên `gw-tick` (100 ms) và gộp vào `LoadShedLadder`, một lớp thuần có đồng hồ inject. Level hiện hành được publish vào `NodeState.shedLevel`, chỗ mà M2b đã đọc sẵn (pacer BULK, NACK_SHED, `request(n)`).
- **Thay đổi session:** đi qua task trên owner loop như các milestone trước. Deadline mới (recycle, slow consumer) nằm trong `Session.nextDeadlineSec()` và `DeadlineWheel` sẵn có, không tạo timer riêng cho từng kết nối.
- **Drain và warm-up:** `DrainCoordinator` và `WarmUpRunner` nằm quanh `NettyGatewayServer.start/stop` (phase `MAX − 100`).

**Tech Stack:** Như M2b. Không thêm dependency. CPU đọc qua `com.sun.management.OperatingSystemMXBean`, direct memory qua `io.netty.util.internal.PlatformDependent`.

**Spec:** `docs/superpowers/specs/2026-11-03-gateway-service-design.md`. Plan này là phần đầu của **M3** (§12): M3a bảo vệ và vòng đời; M3b là TLS reload, SSE, PROXY, tenant cap, trace; M3c là fuzz, JMH, image prod, ADR, release. Các mục chính: §3.5, §5.5 bước 3, §5.9, §7.1–§7.6, §7.8, §8, §9.

## Global Constraints

**Kế thừa từ M1, M2a, M2b**
- Không block trên `gw-io-*`. Mọi thay đổi `Session` chạy trên owner loop. Bật `-Dpangate.assertLoop=true` và BlockHound trong test.
- Metric đặt tên dạng chấm. Conventional Commits.
- Chạy impact trước khi sửa symbol, `detect-changes` trước khi commit.
- Heap/session < 5120 B (đo cuối M2b: 4870 B). `Session` chỉ được thêm tối đa 2 field `long` và 1 field `boolean`.

**Ngưỡng ladder** (spec §7.3, nguyên văn)

| Level | Vào khi (bất kỳ) | Ra khi |
|---|---|---|
| L1 | budget > 0,70 · lag p99 > 50 ms · CPU > 0,70 | budget < 0,60 **và** lag < 30 ms **và** CPU < 0,60 liên tục 30 s |
| L2 | buffer Kafka > 0,70 | buffer < 0,50 |
| L3 | CPU > 0,80 · số session HANDSHAKING tăng liên tục 5 s | CPU < 0,70 liên tục 60 s |
| L4 | budget > 0,90 | budget < 0,80 |
| L5 | budget ≥ 1,00 · direct memory > 0,85 × max | hết điều kiện vào |

Level hiện hành là level cao nhất đang active. Level 6 (kill switch của operator) **ngoài phạm vi**, vì `KillSwitches` trong `control.proto` không có field cho nó.

**Hành động theo level**
- L1: drop EPHEMERAL tới channel unwritable (đã có), dừng BULK (đã có).
- L2: drop upstream telemetry (`gw.upstream{result=dropped}`), áp dụng cho mọi type trừ `cmd` và `cmd_result`.
- L3: admission × 0,5; tạm dừng recycling; `UpgradeGuard` trả 503 kèm `Retry-After` ngẫu nhiên 5–30 s.
- L4: ngừng `request(1)` và trả `NACK_SHED` (đã có từ M2b).
- L5: mỗi loop đóng 4008 các session có outbound pending lớn nhất, cho tới khi loop < 0,9 × `budgetShare`.

**Outbound budget**
- Giá trị hiệu lực = `min(gateway.outbound-budget, 0,35 × PlatformDependent.maxDirectMemory())`. Nếu bị hạ thì ghi INFO.
- `budgetShare = budget / loopCount`.
- Lấy mẫu mỗi 100 ms.

**Slow consumer**
- Channel unwritable liên tục `gateway.limits.slow-consumer-timeout` (mặc định 30 s) → close 4008, pending → `NACK_SLOW_CONSUMER`.
- Channel ghi được trở lại thì xóa deadline.

**Recycling**
- `recycleAtSec = activation + U[min, max]` (mặc định 30 m–90 m).
- Đến hạn, nếu `shedLevel < 3` và tenant bật `recycle.enabled`: gửi `RECONNECT{delay_ms ∈ [0, 5000]}`, state → DRAINING, `drainDeadline = now + delay + grace` (grace 30 s).
- Không đủ điều kiện: dời `recycleAtSec` thêm `U[60, 300]` s.

**Drain** (spec §7.5)
1. `draining = true`, `/ready` trả 503, `SET node:draining:{id} 1 EX 900`. Close listener sau `listener-close-delay` (10 s).
2. Mỗi `interval` (5 s), mỗi loop gửi `RECONNECT{delay ∈ [0, max-delay=60 s]}` cho `ceil(batch-percent(2) × activeAtStart / 100)` session ACTIVE.
3. Kết thúc khi tổng session = 0, hoặc hết `max-duration` (10 phút) thì đóng 1001 phần còn lại.
4. Gọi lặp lại không có tác dụng: luôn trả cùng một future.

**Warm-up** (spec §7.6)
- Chạy sau khi bind, trước khi ready: `iterations` (2000) vòng TLS → WS → HELLO → PING → close, concurrency 32, tổng tối đa 60 s. Hết hạn thì ghi WARN rồi vẫn chuyển sang ready.
- Token do `WarmUpTokens` ký bằng key ES256 sinh trong process, chỉ được chấp nhận từ loopback.
- Session warm-up bỏ qua registry, Kafka, seq Redis và metric nghiệp vụ.
- Ramp admission bắt đầu sau warm-up.

**Kill switch**
- `admission.ratePerNode` (scope fleet hoặc cell) thay `base` của admission, áp dụng trong ≤ 1 s.
- `recycle.enabled` theo tenant. **Mặc định đổi thành `true`.** Spec §7.8 mô tả đây là công tắc để tắt một tính năng đang bật; mặc định `false` từ M2a làm recycling không bao giờ chạy.

**Cấu hình theo profile** (quyết định của plan, để giữ R3 "thoát ≤ 15 s")
- Profile `local` và `dev`: `gateway.drain.max-duration=5s`, `listener-close-delay=0s`, `interval=1s`.
- `local`: `outbound-budget=256MB`, `warmup.iterations=50`.
- `dev`: `outbound-budget=1GB`, `warmup.iterations=200`.
- Prod: `spring.lifecycle.timeout-per-shutdown-phase=11m`, vì drain có thể kéo tới 10 phút. Compose dev: gateway có `stop_grace_period: 20s`.

**Metric mới** (spec §8)
- `gw.shed.level`, `gw.outbound.budget.ratio`, `gw.pending.outbound.bytes`, `gw.direct.memory.bytes`.
- `gw.unwritable.channels`, `gw.slow.consumer.closes{reason=unwritable|overflow|shed}`.
- `gw.drain.reconnects`, `gw.recycle.reconnects`, `gw.handshakes{result=warmup}`.
- `gw.kernel.listen.overflows`, `gw.kernel.listen.drops`, `gw.kernel.tcp.backlog.drop`, `gw.kernel.tcp.mem.pages`.

## Review Focus

1. **SIGTERM khi đang có client kết nối ở local hoặc dev.** Process phải thoát trong ≤ 15 s (drain bị giới hạn 5 s). Test: Task 11, `local-start-check.sh` giữ một client mở trong lúc stop; Task 6 `maxDurationForceClosesWith1001`.
2. **Client bỏ qua RECONNECT trong lúc drain.** Client bị đóng 1001 ở `drainDeadline` hoặc khi hết `max-duration`, và pending trả `NACK_CLOSED`. Test: Task 6 `maxDurationForceClosesWith1001`; Task 10 `drainSendsReconnectAndForceClosesStragglers`.
3. **Tín hiệu nhiễu quanh ngưỡng.** Level không được nhảy lên xuống. L1 chỉ hạ khi điều kiện ra đúng liên tục 30 s; một mẫu vượt ngưỡng giữa chừng phải đặt lại đồng hồ. Test: Task 3 `level1ExitNeedsThirtySecondsOfCalm`, `spikeDuringCalmRestartsTheClock`.
4. **Token warm-up dùng từ địa chỉ không phải loopback, hoặc JWT thật khai tenant `__warmup`.** Cả hai bị từ chối 4001. Test: Task 7 `warmUpTokenFromRemoteIsRejected`, `realJwtClaimingWarmUpTenantIsRejected`.
5. **Máy dev có direct memory nhỏ.** Budget bị hạ về 35% direct memory, nên lúc idle không rơi vào level 5; channel ghi được trở lại trước 30 s thì không bị đóng. Test: Task 1 `effectiveBudgetIsCappedAt35PercentOfDirectMemory`; Task 2 `writableAgainClearsTheDeadline`.

---

## File Structure

```
gateway-service/src/main/java/com/pangate/gateway/
├── config/    GatewayProperties (+outboundBudget, Recycling, Drain, Warmup, limits.slowConsumerTimeout), GatewayLimits,
│              ProtectionConfig (bean mới: budget, ladder sampler, recycler, drain, warm-up, kernel stats)
├── shed/      OutboundBudget, LoadShedLadder, HandshakeTrend, ShedSampler, ShedEnforcer (mới); AdmissionController (sửa)
├── session/   Session (+recycleAtSec, unwritableSinceSec, dirty), SessionTracker (handshaking count, warm-up skip)
├── loop/      LoopContext (+dirty list, outboundBytes, handshaking, unwritable count, recycler)
├── lifecycle/ DrainCoordinator, Recycler, WarmUpRunner (mới)
├── auth/      WarmUpTokens (mới); JwtVerifier (sửa)
├── metrics/   KernelStatsReader (mới)
├── server/    NettyGatewayServer (drain + warm-up), NodeState (+warmedUp)
├── registry/  NodeIdentity (+markDraining)
├── pipeline/  SessionHandler (writability), UpgradeGuard (shed ≥ 3), HelloAuthHandler (warm-up)
├── upstream/  UpstreamPublisher (shed ≥ 2)
├── downstream/ PushDelivery, BroadcastPacer (markDirty)
└── mgmt/      AdminController (gọi drain)
gateway-service/src/it/java/com/pangate/gateway/it/GatewayProtectionIT.java
gateway-devtools/src/main/java/com/pangate/devtools/client/GatewayClient.java   (+pauseReading/resumeReading)
platform-devenv/{docker-compose.yml, scripts/local-start-check.sh}, READMEs, spec §7.5/§7.3/§9
```

---

### Task 1: Cấu hình M3a và `OutboundBudget`

**Files:**
- Modify: `config/GatewayProperties.java`, `config/GatewayLimits.java`, `server/NodeState.java`, `loop/LoopContext.java`, `session/Session.java`, `downstream/PushDelivery.java`, `downstream/BroadcastPacer.java`, `application{,-local,-dev}.yml`
- Create: `shed/OutboundBudget.java`, `config/ProtectionConfig.java`
- Test: `config/GatewayPropertiesTest.java`, `shed/OutboundBudgetTest.java`, `downstream/PushDeliveryTest.java` (bổ sung)

**Interfaces:**
- Produces:
  - `GatewayProperties`:
    - `+ DataSize outboundBudget`
    - `+ Recycling recycling`, với `record Recycling(Duration min, Duration max, Duration grace)`
    - `+ Drain drain`, với `record Drain(int batchPercent, Duration interval, Duration maxDelay, Duration maxDuration, Duration listenerCloseDelay)`
    - `+ Warmup warmup`, với `record Warmup(int iterations, int concurrency, Duration timeout)`
    - `Limits` thêm `Duration slowConsumerTimeout`
  - `GatewayLimits` thêm `Duration slowConsumerTimeout`.
  - `NodeState`: `boolean warmedUp()` và `void warmedUp(boolean)` (volatile).
  - `LoopContext` (các method trên loop, trừ khi ghi chú khác):
    - `void markDirty(Session s)`: thêm vào danh sách dirty nếu `!s.dirty()`.
    - `void sampleOutbound()`: cộng `totalPendingWriteBytes` của các session dirty. Session còn > 0 thì giữ trong danh sách cho lượt sau, ngược lại gỡ ra và `s.dirty(false)`. Ghi tổng vào `outboundBytes`.
    - `long outboundBytes()`: đọc opaque, dùng được từ mọi thread.
    - `List<Session> dirtySessions()`: view chỉ đọc, cho `ShedEnforcer`.
  - `Session`:
    - `boolean dirty()` và `void dirty(boolean)`.
    - `long pendingOutboundBytes()`: `channel.unsafe().outboundBuffer()`, trả 0 nếu `null`.
  - `PushDelivery.write` và `BroadcastPacer.write` gọi `loop.markDirty(session)` sau mỗi lần ghi.
  - `OutboundBudget`:
    ```java
    public final class OutboundBudget {
      public OutboundBudget(long configuredBytes, long maxDirectMemory, LoopRegistry loops, GatewayMetrics m);
      static long effective(long configured, long maxDirect);   // min(configured, 35% maxDirect)
      public long budgetBytes();
      public long shareBytes();                                  // budget / loops
      public void sample();       // gw-tick: execute(loop::sampleOutbound) trên từng loop; ghi lại tổng lần trước
      public double ratio();      // tổng outbound đã đo / budget
    }
    ```
    Gauge `gw.outbound.budget.ratio`, `gw.pending.outbound.bytes`, `gw.direct.memory.bytes` (`PlatformDependent.usedDirectMemory()`).
  - Giá trị mặc định (`application.yml`):
    - `outbound-budget: 4GB`
    - `recycling: { min: 30m, max: 90m, grace: 30s }`
    - `drain: { batch-percent: 2, interval: 5s, max-delay: 60s, max-duration: 10m, listener-close-delay: 10s }`
    - `warmup: { iterations: 2000, concurrency: 32, timeout: 60s }`
    - `limits.slow-consumer-timeout: 30s`
    - `spring.lifecycle.timeout-per-shutdown-phase: 11m`
  - Profile `local` và `dev` theo Global Constraints.

- [ ] **Step 1: Viết test thất bại**

```java
// GatewayPropertiesTest
@Test void bindsProtectionDefaults() { /* outboundBudget 4GB; recycling 30m/90m/30s; drain 2,5s,60s,10m,10s; warmup 2000/32/60s;
    limits.slowConsumerTimeout 30s; env spring.lifecycle.timeout-per-shutdown-phase == "11m" */ }
@Test void localAndDevKeepShutdownUnder15s() { /* local: drain.maxDuration 5s, listenerCloseDelay 0s, interval 1s, budget 256MB, warmup 50;
    dev: budget 1GB, warmup 200, cùng drain */ }
// OutboundBudgetTest — LoopFixture 1 loop
@Test void effectiveBudgetIsCappedAt35PercentOfDirectMemory() {
  assertThat(OutboundBudget.effective(4L << 30, 1L << 30)).isEqualTo((long) (0.35 * (1L << 30)));
  assertThat(OutboundBudget.effective(256L << 20, 4L << 30)).isEqualTo(256L << 20); }
@Test void sampleSumsPendingBytesOfDirtySessions() {
  /* 2 session; EmbeddedChannel không flush: write 1000 B vào s1, 500 B vào s2, markDirty cả hai; sample(), runPendingTasks;
     sample() lần nữa → ratio == 1500 / budget; flush s1 → sau 2 lần sample: outboundBytes == 500, s1.dirty()==false */ }
// PushDeliveryTest
@Test void deliveredSessionIsMarkedDirty() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service test -Dtest='GatewayPropertiesTest,OutboundBudgetTest,PushDeliveryTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực theo Interfaces**

Wiring trong bean mới `ProtectionConfig`:
- Bean `OutboundBudget(p.outboundBudget().toBytes(), PlatformDependent.maxDirectMemory(), loops, metrics)`.
- Lên lịch `tick.scheduleAtFixedRate(budget::sample, 100 ms)`.
- Cập nhật mọi chỗ khởi tạo `GatewayLimits` và `GatewayProperties` trong test (`TestLimits`, `UpgradeGuardTest`).

- [ ] **Step 4: Chạy toàn bộ unit**

Run: `./mvnw -q -f gateway-service test`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): M3a configuration and outbound budget sampling"
```

---

### Task 2: `SlowConsumerPolicy` (deadline unwritable)

**Files:**
- Modify: `session/Session.java`, `pipeline/SessionHandler.java`, `loop/LoopContext.java`, `session/SessionTracker.java`, `downstream/PushDelivery.java`, `metrics/GatewayMetrics.java`
- Test: `session/SlowConsumerTest.java`

**Interfaces:**
- Consumes: `GatewayLimits.slowConsumerTimeout` (Task 1).
- Produces:
  - `Session`:
    - `void writabilityChanged(boolean writable, long nowSec)`: unwritable thì đặt `unwritableSinceSec = now` và tăng `loop.unwritable`; writable thì xóa (`Long.MAX_VALUE`) và giảm. Cả hai trường hợp đều gọi `reschedule()`.
    - `nextDeadlineSec()` cho ACTIVE và DRAINING thêm `unwritableSinceSec + slowConsumerTimeout`.
    - `onDeadline`: deadline này đến hạn → `close(SLOW_CONSUMER, "SLOW_CONSUMER")`.
  - `SessionHandler.channelWritabilityChanged(ctx)` gọi `session.writabilityChanged(ch.isWritable(), loop.nowSec())` rồi chuyển tiếp event.
  - `SessionTracker.onClosed`: nếu session đóng khi đang unwritable thì giảm `loop.unwritable`.
  - Gauge `gw.unwritable.channels` (tổng các loop). Counter `gw.slow.consumer.closes{reason=unwritable}`. Đóng do `PENDING_OVERFLOW` (M2b) thì đếm `reason=overflow`.

- [ ] **Step 1: Viết test thất bại**

```java
// SlowConsumerTest — LoopFixture, session ACTIVE, 1 push RELIABLE đang pending (StreamRef ghi lại)
@Test void unwritableFor30sClosesWith4008AndNacksSlowConsumer() {
  /* channel.config().setWriteBufferWaterMark(1,2) + write không flush → fireChannelWritabilityChanged;
     advanceTo(now+29) → ACTIVE; advanceTo(now+30) → CLOSED, closeCode SLOW_CONSUMER, ack NACK_SLOW_CONSUMER,
     gw.slow.consumer.closes{reason=unwritable}==1 */ }
@Test void writableAgainClearsTheDeadline() { /* unwritable 20 s, flush → writable; advanceTo(+40) → vẫn ACTIVE; gw.unwritable.channels == 0 */ }
@Test void closeWhileUnwritableResetsTheGauge() { }
@Test void pendingOverflowCloseIsCountedAsOverflow() { }
```

- [ ] **Step 2–4:** Chạy `./mvnw -q -f gateway-service test -Dtest='SlowConsumerTest,SessionTest,PushDeliveryTest'`: FAIL, hiện thực, rồi PASS cả unit suite.

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(gateway): close slow consumers unwritable for 30 s with 4008 and NACK_SLOW_CONSUMER"
```

---

### Task 3: `LoadShedLadder` và `HandshakeTrend` (lớp thuần)

**Files:**
- Create: `shed/LoadShedLadder.java`, `shed/HandshakeTrend.java`
- Test: `shed/LoadShedLadderTest.java`, `shed/HandshakeTrendTest.java`

**Interfaces:**
- Produces:
  ```java
  public final class LoadShedLadder {
    public record Signals(double budgetRatio, double lagP99Sec, double cpu, double kafkaBufferRatio,
        boolean handshakesRising, double directMemoryRatio) {}
    public LoadShedLadder(LongSupplier monoMillis);
    /** Mỗi 100 ms. Trả level 0–5 theo bảng Global Constraints; mỗi level có trạng thái active và hysteresis riêng. */
    public int evaluate(Signals s);
  }
  public final class HandshakeTrend {
    /** Gọi mỗi 100 ms với tổng session HANDSHAKING; lấy một mẫu mỗi giây. */
    public void record(long monoMillis, int handshaking);
    /** true khi 6 mẫu giây gần nhất tăng ngặt (= "tăng liên tục 5 s"). */
    public boolean rising();
  }
  ```
- Thuật toán hysteresis (cho L1 và L3; các level khác ra ngay khi điều kiện ra đúng): `calmSinceMs = -1`. Mỗi lần evaluate:
  - Điều kiện vào đúng → active, `calmSinceMs = -1`.
  - Đang active và điều kiện ra đúng → `calmSinceMs` (nếu đang -1) = now. Nếu `now − calmSinceMs ≥` 30 000 (L1) hoặc 60 000 (L3) thì hết active.
  - Đang active và điều kiện ra sai → `calmSinceMs = -1`.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void quietSignalsAreLevel0() { }
@Test void eachEntryConditionSelectsItsLevel() {
  /* budget .71 → 1; lag .051 → 1; cpu .71 → 1; kafka .71 → 2; cpu .81 → 3; rising → 3; budget .91 → 4; budget 1.0 → 5; direct .86 → 5 */ }
@Test void highestActiveLevelWins() { /* kafka .8 + budget .95 → 4 */ }
@Test void level1ExitNeedsThirtySecondsOfCalm() { /* vào bằng budget .75; budget .5 lag 0 cpu .1 trong 29,9 s → 1; tại 30 s → 0 */ }
@Test void spikeDuringCalmRestartsTheClock() { /* calm 20 s, một mẫu budget .65 (không < .60), calm 29 s → vẫn 1 */ }
@Test void level2And4ExitImmediatelyBelowTheirExitThreshold() { /* kafka .75 → 2; .6 → vẫn 2; .49 → 0. budget .95 → 4; .85 → vẫn ≥ 1 (L1 giữ); level4 không còn */ }
@Test void level3ExitNeedsSixtySecondsOfCpuBelow70() { }
@Test void level5DropsAsSoonAsConditionClears() { }
// HandshakeTrendTest
@Test void strictlyRisingForSixSecondSamplesIsRising() { }
@Test void aFlatOrFallingSecondBreaksTheTrend() { }
```

- [ ] **Step 2–4:** Chạy `./mvnw -q -f gateway-service test -Dtest='LoadShedLadderTest,HandshakeTrendTest'`: FAIL, hiện thực, PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(gateway): load-shedding ladder with per-level hysteresis and handshake trend"
```

---

### Task 4: Nối ladder — `ShedSampler`, hành động L2/L3/L5, admission

**Files:**
- Create: `shed/ShedSampler.java`, `shed/ShedEnforcer.java`
- Modify: `shed/AdmissionController.java`, `upstream/UpstreamPublisher.java`, `pipeline/UpgradeGuard.java`, `session/SessionTracker.java` + `loop/LoopContext.java` (đếm HANDSHAKING), `config/ProtectionConfig.java`, `config/GatewayConfig.java` (bean admission)
- Test: `shed/ShedSamplerTest.java`, `shed/ShedEnforcerTest.java`, `shed/AdmissionControllerTest.java`, `upstream/UpstreamPublisherTest.java`, `pipeline/UpgradeGuardTest.java`

**Interfaces:**
- Consumes: `OutboundBudget` (Task 1), `LoadShedLadder`, `HandshakeTrend` (Task 3), `EventLoopLagProbe.maxP99Seconds()`, `KafkaClients.maxBufferRatio()`, `TenantLimitsStore.policy(null).admissionRatePerNode()`.
- Produces:
  - `LoopContext`: `int handshaking()` (opaque) và `void addHandshaking(int)`. `SessionTracker.admit` gọi +1; `Session.activate` gọi −1; `SessionTracker.onClosed` gọi −1 khi `identity() == null` (session chưa từng activate).
  - `ShedSampler`:
    ```java
    public final class ShedSampler {
      public ShedSampler(LoadShedLadder ladder, OutboundBudget budget, DoubleSupplier lagP99, DoubleSupplier cpu,
          DoubleSupplier kafkaRatio, DoubleSupplier directRatio, LoopRegistry loops, HandshakeTrend trend,
          NodeState node, ShedEnforcer enforcer, LongSupplier monoMillis, GatewayMetrics m);
      public void sample();   // gw-tick, mỗi 100 ms: dựng Signals → node.shedLevel(level); level 5 → enforcer.relieve()
    }
    ```
    `cpu` lấy từ `OperatingSystemMXBean.getProcessCpuLoad()`, giá trị âm coi là 0. `directRatio = usedDirectMemory / maxDirectMemory`. Gauge `gw.shed.level`. Đổi level thì ghi INFO kèm các tín hiệu.
  - `ShedEnforcer`:
    - `void relieve()`: trên từng loop (qua `execute`), lấy các session dirty, sắp theo `pendingOutboundBytes` giảm dần, đóng `SLOW_CONSUMER` với reason `"SHED"` cho tới khi phần còn lại < 0,9 × `budget.shareBytes()`.
    - Counter `gw.slow.consumer.closes{reason=shed}`.
  - `AdmissionController`:
    - Constructor thêm `IntSupplier shedLevel`.
    - `effectiveRate()` nhân 0,5 khi shed ≥ 3.
    - `void applyKillSwitch(int ratePerNode)` (thay `overrideBaseRate`; giá trị 0 xóa override).
    - Một task gw-tick mỗi 1 s gọi `applyKillSwitch(tenants.policy(null).admissionRatePerNode())`.
  - `UpstreamPublisher`: thêm điều kiện drop telemetry `node.shedLevel() >= 2`, kết quả `dropped`.
  - `UpgradeGuard`: thêm điều kiện 503 `node.shedLevel() >= 3` (cùng nhánh với soft cap).

- [ ] **Step 1: Viết test thất bại**

```java
// ShedSamplerTest — ladder thật, supplier giả, NodeState thật, 2 loop LoopFixture
@Test void levelIsPublishedToNodeStateAndGauge() { }
@Test void level5RunsTheEnforcer() { }
@Test void handshakingCountFeedsTheTrend() { /* admit 1 session/giây trong 6 s (không HELLO) → level 3 */ }
// ShedEnforcerTest — 3 session pending 300/200/100 B; share 400 → đóng session 300 (còn 300 < 360), các session khác giữ nguyên
@Test void closesLargestPendingUntilBelow90PercentOfShare() { }
// AdmissionControllerTest
@Test void shedLevel3HalvesTheRate() { }
@Test void killSwitchReplacesAndZeroRestoresBase() { }
// UpstreamPublisherTest
@Test void telemetryDroppedAtShedLevel2ButCmdStillProduced() { }
// UpgradeGuardTest
@Test void shedLevel3Answers503WithRetryAfter() { }
```

- [ ] **Step 2–4:** Chạy `./mvnw -q -f gateway-service test`: FAIL (compile), hiện thực, PASS toàn bộ.

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(gateway): wire the shed ladder: telemetry drop, admission halving, 503 at level 3, level-5 relief"
```

---

### Task 5: Recycling (`Recycler`) và kill switch `recycle.enabled`

**Files:**
- Create: `lifecycle/Recycler.java`
- Modify: `session/Session.java`, `loop/LoopContext.java` (`recycler()` và setter), `control/TenantPolicy.java` (mặc định `recycleEnabled=true`), `config/ProtectionConfig.java`
- Test: `lifecycle/RecyclerTest.java`, `control/TenantLimitsStoreTest.java` (sửa giá trị mặc định)

**Interfaces:**
- Produces:
  ```java
  public final class Recycler {
    public Recycler(GatewayProperties.Recycling cfg, NodeState node, TenantLimitsStore tenants, RandomGenerator rnd,
        GatewayMetrics m);
    public long firstRecycleAtSec(long nowSec);        // now + U[min, max] (giây)
    /** Trên loop: đủ điều kiện → s.drain(U[0,5000] ms, false, grace) và trả Long.MAX_VALUE; ngược lại now + U[60,300]. */
    public long onDue(Session s, long nowSec);
  }
  ```
  - `Session`:
    - Thêm field `recycleAtSec`.
    - `activate` đặt `recycleAtSec = loop.recycler() == null ? MAX : loop.recycler().firstRecycleAtSec(now)`. Session warm-up không recycle (Task 7 đặt về MAX).
    - `nextDeadlineSec` (ACTIVE) thêm `recycleAtSec`.
    - `onDeadline`: ACTIVE và `now ≥ recycleAtSec` → `recycleAtSec = loop.recycler().onDue(this, now)`, sau đó `reschedule()`.
    - Thêm overload `drain(long delayMs, boolean reBootstrap, long graceSec)`. Overload cũ dùng grace 30 s.
  - Counter `gw.recycle.reconnects`.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void firstRecycleIsBetweenMinAndMax() { /* rnd cố định; min 30m max 90m → [now+1800, now+5400] */ }
@Test void dueSessionGetsReconnectAndDrains() { /* advanceTo(recycleAt) → RECONNECT delay ≤ 5000, state DRAINING, drainDeadline = now + delay + 30 */ }
@Test void shedLevel3PostponesBy60To300s() { }
@Test void tenantWithRecycleDisabledIsPostponed() { }
@Test void defaultPolicyHasRecycleEnabled() { }
```

- [ ] **Step 2–4:** Chạy `./mvnw -q -f gateway-service test -Dtest='RecyclerTest,SessionTest,TenantLimitsStoreTest'`: FAIL, hiện thực, PASS cả unit suite.

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(gateway): connection recycling with shed and tenant gating"
```

---

### Task 6: `DrainCoordinator`, `NettyGatewayServer.stop`, `/admin/drain`

**Files:**
- Create: `lifecycle/DrainCoordinator.java`
- Modify: `server/NettyGatewayServer.java`, `mgmt/AdminController.java`, `registry/NodeIdentity.java` (+`markDraining()`), `config/GatewayConfig.java`, `config/ProtectionConfig.java`
- Test: `lifecycle/DrainCoordinatorTest.java`, `mgmt/AdminControllerTest.java`

**Interfaces:**
- Produces:
  ```java
  public final class DrainCoordinator {
    public DrainCoordinator(GatewayProperties.Drain cfg, LoopRegistry loops, NodeState node, Runnable markDrainingInRedis,
        ScheduledExecutorService scheduler, RandomGenerator rnd, GatewayMetrics m);
    /** Idempotent: lần gọi đầu bắt đầu drain, mọi lần sau trả cùng future. */
    public CompletableFuture<Void> drain(Runnable closeListeners);
  }
  ```
  - Thứ tự thực hiện (spec §7.5):
    1. `node.draining(true)`, rồi `markDrainingInRedis.run()`.
    2. Lên lịch `closeListeners` sau `listenerCloseDelay`.
    3. Chụp `activeAtStart` của từng loop (`loop.activeCount()`).
    4. Mỗi `interval`, mỗi loop chạy (qua `execute`): `n = ceil(batchPercent × activeAtStart / 100)`, tối thiểu 1 khi loop còn session. Gọi `s.drain(rnd ∈ [0, maxDelay], false)` cho `n` session ACTIVE đầu tiên; đếm `gw.drain.reconnects`.
    5. Mỗi `interval` kiểm `loops.totalActive() == 0`: đúng → hủy lịch và complete.
    6. Hết `maxDuration` → mỗi loop `close(GOING_AWAY, "DRAIN_TIMEOUT")` mọi session còn lại, rồi complete khi tổng về 0 hoặc thêm tối đa 5 s.
  - `NodeIdentity.markDraining()`: `SET node:draining:{id} 1 EX 900` (async, lỗi chỉ ghi WARN). Key tạo bằng `RegistryKeys`.
  - `NettyGatewayServer`:
    - `CompletableFuture<Void> drain()` = `coordinator.drain(this::closeListeners)`.
    - `stop(callback)` = `drain().whenComplete(→ io.shutdown(0, 5 s) → callback)`. Không còn đóng 1001 toàn bộ ngay lập tức.
  - `AdminController` nhận `Supplier<CompletableFuture<Void>>` và gọi `server.drain()`, **không** dừng process. preStop gọi drain, SIGTERM sau đó chờ cùng future.

- [ ] **Step 1: Viết test thất bại**

```java
// DrainCoordinatorTest — 2 loop DefaultEventLoop, 10 session/loop (EmbeddedChannel giả lập qua LoopFixture), scheduler thủ công
@Test void drainMarksNodeAndRedisAndClosesListenersAfterDelay() { }
@Test void eachIntervalReconnectsBatchPercentOfActiveAtStart() { /* batch 20%, 10 session → 2 RECONNECT mỗi loop mỗi interval */ }
@Test void completesWhenNoSessionsRemain() { }
@Test void maxDurationForceClosesWith1001() { /* client bỏ qua RECONNECT → hết maxDuration đóng 1001 "DRAIN_TIMEOUT"; pending → NACK_CLOSED; future complete */ }
@Test void secondCallReturnsTheSameFuture() { }
@Test void emptyNodeCompletesImmediately() { }
// AdminControllerTest
@Test void loopbackDrainStartsDrainWithoutStopping() { }
```

- [ ] **Step 2–4:** Chạy `./mvnw -q -f gateway-service test -Dtest='DrainCoordinatorTest,AdminControllerTest'`: FAIL, hiện thực, PASS. Sau đó chạy `./mvnw -q -f gateway-service verify -Dit.test='GatewayLocalIT,GatewayClusterIT'` (IT dừng gateway qua `ctx.close()` cần vẫn dừng được): PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(gateway): staged drain coordinator for SIGTERM and /admin/drain"
```

---

### Task 7: `WarmUpTokens` và đường xác thực warm-up

**Files:**
- Create: `auth/WarmUpTokens.java`
- Modify: `auth/JwtVerifier.java`, `pipeline/HelloAuthHandler.java`, `session/SessionTracker.java`, `session/Session.java` (`warmUp()`), `config/GatewayConfig.java`
- Test: `auth/WarmUpTokensTest.java`, `pipeline/HelloAuthHandlerTest.java` (bổ sung)

**Interfaces:**
- Produces:
  ```java
  public final class WarmUpTokens {
    public static final String TENANT = "__warmup";
    public WarmUpTokens(String audience, String issuer);   // sinh cặp key ES256 trong process, kid "warmup-<uuid>"
    public String kid();
    public java.security.interfaces.ECPublicKey publicKey();
    public String mint(int i, long nowSec);                  // sub "__warmup:<i>", device "warmup-<i>", tenant TENANT, exp now+60
    public static boolean isWarmUp(String tenantId);
  }
  ```
  - `JwtVerifier`: constructor thêm `WarmUpTokens` (nullable).
    - `kid` bằng `warmUp.kid()` → verify bằng `warmUp.publicKey()`.
    - JWT ký bằng key khác mà khai `tenant == TENANT` → `Rejected("reserved tenant")`.
  - `HelloAuthHandler`:
    - Claims warm-up mà remote address không phải loopback → fail `AUTH_FAILED`.
    - Session warm-up: không đọc seq (`seq_unknown=true` ngay lập tức), metric `handshake("warmup")` thay cho `"ok"`.
  - `Session.warmUp()` = `WarmUpTokens.isWarmUp(identity.tenantId())`.
  - `SessionTracker`: với session warm-up, `onActivated` và `onClosed` bỏ qua listener (registry, lifecycle) và `metrics.closed`. `recycleAtSec = MAX`.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void warmUpTokenVerifiesAndClaimsReservedTenant() { }
@Test void warmUpTokenFromRemoteIsRejected() { /* EmbeddedChannel với remote 10.0.0.1 → close 4001 */ }
@Test void realJwtClaimingWarmUpTenantIsRejected() { /* JWT ký bằng key dev với tenant "__warmup" → 4001 */ }
@Test void warmUpSessionSkipsListenersSeqAndBusinessMetrics() {
  /* listener ghi lại không được gọi; SeqReader giả không được gọi; gw.handshakes{result=warmup}==1, {result=ok}==0 */ }
```

- [ ] **Step 2–4:** Chạy `./mvnw -q -f gateway-service test -Dtest='WarmUpTokensTest,HelloAuthHandlerTest,JwtVerifierTest'`: FAIL, hiện thực, PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(gateway): in-process warm-up tokens accepted from loopback only"
```

---

### Task 8: `WarmUpRunner`, thứ tự khởi động và readiness

**Files:**
- Create: `lifecycle/WarmUpRunner.java`
- Modify: `server/NettyGatewayServer.java` (bind → warm-up → `warmedUp` → `admission.markReady`), `config/GatewayConfig.java` (`ReadinessCheck` "warm-up not finished"), `config/ProtectionConfig.java`
- Test: `lifecycle/WarmUpRunnerTest.java` (dựng server Netty thật trên port 0 bằng pipeline của gateway và `LoopFixture`)

**Interfaces:**
- Consumes: `WarmUpTokens.mint` (Task 7), `SslContextProvider` (dev cert).
- Produces:
  ```java
  public final class WarmUpRunner {
    public record Result(int ok, int failed, boolean timedOut) {}
    public WarmUpRunner(GatewayProperties.Warmup cfg, int wsPort, String wsPath, WarmUpTokens tokens, LongSupplier wallSec);
    /** Chặn tối đa cfg.timeout; client Netty NIO riêng (1 thread), trust-all, chỉ kết nối 127.0.0.1; luôn trả kết quả. */
    public Result run();
  }
  ```
  - Mỗi vòng: TLS → WS upgrade (subprotocol `pangate.v1`) → HELLO (`mint(i)`) → chờ CONNECTED → PING → chờ PONG → close 1000.
  - Concurrency giới hạn bằng `Semaphore(cfg.concurrency)`.
  - `iterations == 0` → bỏ qua, trả `Result(0, 0, false)`.
  - Kết thúc: ghi INFO `warm-up: ok=…, failed=…, took=… ms`; timeout thì ghi WARN.
  - Trong `NettyGatewayServer.start()`: sau khi bind, chạy `warmUp.run()`, rồi `node.warmedUp(true)`, rồi `admission.markReady()`.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void runsAllIterationsAgainstTheRealPipeline() { /* iterations 20 → ok 20, failed 0; gw.handshakes{result=warmup}==20 */ }
@Test void zeroIterationsIsANoOp() { }
@Test void stopsAtTimeoutAndReportsIt() { /* port không có server (đóng ngay) + timeout 1s → timedOut hoặc failed == iterations, trả trong ≤ 2 s */ }
// GatewayConfig readiness (unit qua HealthIndicatorsTest)
@Test void notReadyUntilWarmedUp() { }
```

- [ ] **Step 2–4:** Chạy `./mvnw -q -f gateway-service test -Dtest='WarmUpRunnerTest,HealthIndicatorsTest'`: FAIL, hiện thực, PASS. Sau đó chạy `./mvnw -q -f gateway-service verify -Dit.test=GatewayLocalIT` (profile local, 50 vòng warm-up): PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(gateway): warm-up runner before readiness; admission ramp starts after it"
```

---

### Task 9: `KernelStatsReader`

**Files:**
- Create: `metrics/KernelStatsReader.java`
- Modify: `config/ProtectionConfig.java`
- Test: `metrics/KernelStatsReaderTest.java` (fixture `src/test/resources/proc/{netstat,sockstat}`)

**Interfaces:**
- Produces:
  ```java
  public final class KernelStatsReader {
    public KernelStatsReader(Path netstat, Path sockstat);
    public static Optional<KernelStatsReader> forThisHost();   // Linux và /proc/net/netstat đọc được; ngược lại empty + INFO
    public void refresh();                                      // gw-tick mỗi 10 s
    public long tcpExt(String field);                           // ListenOverflows, ListenDrops, TCPBacklogDrop; thiếu → 0
    public long tcpMemPages();                                  // dòng "TCP: ... mem N" trong sockstat
    public void bindTo(MeterRegistry r);   // gw.kernel.listen.overflows, .listen.drops, .tcp.backlog.drop, .tcp.mem.pages
  }
  ```
  Định dạng `/proc/net/netstat`: dòng header `TcpExt: <tên…>` theo sau là dòng `TcpExt: <số…>` cùng thứ tự cột.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void parsesTcpExtCountersByName() { }
@Test void parsesSockstatTcpMem() { }
@Test void missingFieldOrFileReadsZeroWithoutThrowing() { }
@Test void notRegisteredWhenNetstatUnreadable() { /* forThisHost với path không tồn tại → empty */ }
```

- [ ] **Step 2–4:** Chạy `./mvnw -q -f gateway-service test -Dtest=KernelStatsReaderTest`: FAIL, hiện thực, PASS.

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(gateway): kernel listen-queue and TCP memory metrics on Linux"
```

---

### Task 10: Devtools `pauseReading` và IT M3a

**Files:**
- Modify: `gateway-devtools/.../client/GatewayClient.java` (+`pauseReading()`, `resumeReading()`), test của nó
- Create: `gateway-service/src/it/java/com/pangate/gateway/it/GatewayProtectionIT.java`

**Interfaces:**
- Produces:
  - `GatewayClient.pauseReading()` và `resumeReading()`: gọi `channel.config().setAutoRead(false/true)`.
  - Mỗi test dùng một gateway riêng, có helper `start(String... extra)` tương tự `GatewayPushIT.Gw`, dùng chung `FakeRouter`.

- [ ] **Step 1: Viết các kịch bản IT**

```java
@Test void warmUpRunsBeforeReadyAndLeavesNoTrace() {
  /* --gateway.warmup.iterations=20: /ready 200 → prometheus gw_handshakes_total{result="warmup"} 20; Redis không có key reg:{__warmup:*};
     lifecycle không có event tenant "__warmup" */ }
@Test void drainSendsReconnectAndForceClosesStragglers() {
  /* --gateway.drain.interval=1s --gateway.drain.batch-percent=50 --gateway.drain.max-delay=500ms --gateway.drain.max-duration=6s
     --gateway.drain.listener-close-delay=0s; 4 client, 1 client có push RELIABLE chưa ACK và bỏ qua RECONNECT;
     POST /admin/drain → 202; /ready 503 lý do "draining"; GET node:draining:{id} == "1"; cả 4 client nhận RECONNECT trong ≤ 3 s;
     3 client tự đóng; client còn lại bị đóng 1001 trong ≤ 8 s và router nhận NACK_CLOSED; kết nối mới tới cổng ws bị từ chối */ }
@Test void slowConsumerIsClosedWith4008() {
  /* --gateway.limits.slow-consumer-timeout=3s: client pauseReading; 1 push RELIABLE (không ACK) rồi đẩy EPHEMERAL 60 KB tới khi
     channel unwritable (gw_unwritable_channels ≥ 1) → client đóng 4008 trong ≤ 6 s; router nhận NACK_SLOW_CONSUMER */ }
@Test void outboundPressureRaisesShedLevelAndRelievesTheNode() {
  /* --gateway.outbound-budget=256KB --gateway.limits.slow-consumer-timeout=5m: 10 client pauseReading + 1 client đọc bình thường;
     đẩy EPHEMERAL 60 KB tới các client dừng đọc → NodeState.shedLevel() ≥ 4 trong lúc tải; push RELIABLE mới → NACK_SHED;
     level 5 đóng ≥ 1 client dừng đọc với 4008; client đọc bình thường vẫn PING/PONG; sau khi tải dừng → shedLevel ≤ 1 trong ≤ 5 s */ }
@Test void recyclingSendsReconnectWhenDueUnlessTenantDisabled() {
  /* --gateway.recycling.min=2s --gateway.recycling.max=3s: client t1 nhận RECONNECT (re_bootstrap=false) trong ≤ 5 s;
     TenantCfg t2 recycle_enabled=false qua gw.control → client t2 không nhận RECONNECT trong 6 s */ }
@Test void admissionKillSwitchAppliesWithinOneSecond() {
  /* TenantCfg FLEET admission_rate_per_node=1 → trong 1 s, mở 5 kết nối liên tiếp: ≥ 3 bị AcceptGate từ chối (đóng ngay);
     xóa (tombstone) → mở bình thường */ }
```

- [ ] **Step 2: Chạy**

Run: `make bootstrap && ./mvnw -f gateway-service verify`
Expected: mọi unit test và IT xanh, kể cả `GatewayProtectionIT` (6 test).

- [ ] **Step 3: Sửa lỗi lộ ra** trong task sở hữu code đó, rồi chạy lại Step 2.

- [ ] **Step 4: Commit**

```bash
git add gateway-devtools gateway-service
git commit -m "test(gateway): M3a ITs (warm-up, drain, slow consumer, shed, recycling, admission kill switch)"
```

---

### Task 11: R1–R3 với drain, đo đạc, tài liệu

**Files:**
- Modify: `platform-devenv/scripts/local-start-check.sh` (giữ một client mở trong lúc SIGTERM), `platform-devenv/docker-compose.yml` (gateway `stop_grace_period: 20s`), `gateway-service/README.md`, `platform-devenv/README.md`, spec, plan này (mục kết quả).
- Spec cần sửa:
  - §7.5: giá trị drain cho local và dev.
  - §7.3: level 6 hoãn lại vì proto chưa có switch.
  - §7.8: `recycle.enabled` mặc định bật.
  - §9: khóa cấu hình mới (`slow-consumer-timeout`, `drain.listener-close-delay`).

- [ ] **Step 1: Kiểm R1–R3 có drain**

Run: `make bootstrap && make local-check`
Expected: `LOCAL START OK`. Lần dừng có client mở vẫn thoát trong ≤ 15 s; log có dòng `warm-up: ok=50`.

- [ ] **Step 2: Heap, tải, dev**

Run: `make memory-check && make load-check N=18000 && make images && make up && make smoke; make down`
Expected:
- Heap/session < 5120 B.
- Lag p99 < 20 ms và `gw_shed_level 0` trong lúc load-check.
- Compose healthy trong ≤ 90 s, `SMOKE OK`.
- `docker compose stop gateway` thoát trong ≤ 15 s.

- [ ] **Step 3: Ghi kết quả** vào mục `## Kết quả đo M3a`: ready local (kèm warm-up), stop có client, compose healthy, heap/session, lag p99, số test.

- [ ] **Step 4: Commit**

```bash
node .gitnexus/run.cjs detect-changes --scope staged --repo .
git add -A
git commit -m "chore: M3a exit checks with drain and warm-up, docs and measurements"
```

**Tiêu chí hoàn thành M3a** (phần của M3 trong spec §12):
- IT drain, shed và slow consumer xanh.
- R1–R3 vẫn đạt khi có drain và warm-up.
- Heap/session < 5 KB.
- Lag p99 < 20 ms với 18k session và level 0.

## Kết quả đo M3a

Đo trong container phát triển (`ulimit -n` 20000), Docker 1 node Kafka 3.9.1 và Redis 7.4.2.

| Chỉ số | Kết quả | Ngưỡng |
|---|---|---|
| Ready ở local (jar, profile `local`, kể cả warm-up 50 vòng) | 5,9 s; warm-up `ok=50` mất 312 ms | ≤ 30 s (R1) |
| Dừng bằng SIGTERM khi có client mở (client bỏ qua RECONNECT) | 5,5 s (drain đóng 1001 ở mốc 5 s); `make local-check` → `LOCAL START OK` | ≤ 15 s (R3) |
| Compose dev tới healthy / `docker compose stop gateway` | 28 s / 1,3 s; `SMOKE OK (connected, pong, upstream-ack, push-ack, resume)` | ≤ 90 s (R2) / ≤ 15 s |
| Heap/session (10k idle, sau warm-up) | 4.777 B | < 5.120 B |
| Lag p99 với 18k session idle | 0,27 ms; đo lại sau review: 0,62 ms | < 20 ms |
| `gw_shed_level` khi ổn định (đo bằng `make load-check`) | 0. Trong lúc ramp 2000 handshake/s, lag p99 lên 52–105 ms làm node vào L1, rồi tự về 0 sau khoảng 98 s (cửa sổ lag 10 s cộng 30 s yên ổn). Script giờ chờ level 0 tối đa 180 s rồi mới đo | 0 |
| Test | gateway: 420 unit + 84 IT (`GatewayProtectionIT` 6); devtools 30 | xanh |
