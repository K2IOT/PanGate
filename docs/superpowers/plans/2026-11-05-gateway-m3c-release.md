# Gateway M3c (fuzz, JMH, image prod, ADR, release 0.1.0) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Đóng milestone M3 và ra bản `0.1.0`. Cụ thể:
- Leak detection của Netty đặt theo profile.
- Image prod mang các JVM flag đã duyệt.
- Fuzz `FrameCodec` và `HelloAuthHandler` bằng Jazzer, 10 phút mỗi target, không crash và không leak `ByteBuf`.
- Benchmark JMH cho `FrameCodec`, `PrincipalIndex` và `DeadlineWheel`.
- Sửa test flaky `TenantCapBorrowerIT`.
- Viết ADR 0001–0012.
- Bỏ SNAPSHOT, version `0.1.0`.

**Architecture:**
- **Fuzz:** fuzz test là JUnit `@FuzzTest` (jazzer-junit), đặt trong `src/test` để dùng lại `LoopFixture` và `AuthFixture`.
  - Mặc định (regression mode) chỉ chạy các input seed, nên nằm trong `make verify`.
  - `make fuzz` đặt `JAZZER_FUZZ=1` để fuzz thật.
  - Leak được bắt qua log `LEAK:` của `ResourceLeakDetector` (`LeakWatch`), không phụ thuộc thứ tự khởi tạo class.
- **JMH:** profile `-Pjmh` thêm `src/jmh/java` làm test source, chạy `org.openjdk.jmh.Main` qua `exec-maven-plugin` trên classpath test. Không shade jar, không đụng tới bản repackage của Spring Boot.
- **Image:** cùng một image cho dev và prod.
  - Jib `jvmFlags` chỉ chứa flag không phụ thuộc môi trường.
  - Flag theo môi trường (`-Xmx` ở dev, `MaxDirectMemorySize`) do deployment đặt qua `JAVA_TOOL_OPTIONS`.
  - Leak level do config Spring quyết định, không đặt trong image.

**Tech Stack:** Như M3b, thêm:
- `com.code-intelligence:jazzer-junit:0.24.0` (test).
- `org.openjdk.jmh:jmh-core` và `jmh-generator-annprocess` `1.37` (chỉ trong profile `jmh`).
- `org.codehaus.mojo:build-helper-maven-plugin` và `exec-maven-plugin` (version theo Spring Boot parent).

**Spec:** `docs/superpowers/specs/2026-11-03-gateway-service-design.md`. Plan này là phần cuối của **M3** (§12): M3a bảo vệ và vòng đời (xong), M3b phần biên (xong), M3c là plan này. Các mục chính: §2 (version), §9.1 (bảng profile: leak detection, JVM), §10 (`ExitOnOutOfMemoryError`), §11 (fuzz, benchmark), §12 (exit M3), §13 (ADR).

## Global Constraints

**Kế thừa từ M1–M3b**
- Không block trên `gw-io-*` hay `gw-tick`. Test bật BlockHound và `-Dpangate.assertLoop=true`.
- Conventional Commits. Chạy impact trước khi sửa symbol, `detect-changes` trước khi commit.
- Heap/session < 5120 B (M3b đo 4794 B). M3c **không** thêm field nào vào `Session`.

**Leak detection** (spec §9.1, §11)
- Property `gateway.netty.leak-detection`: `disabled` trong `application.yml` (prod), `simple` trong `application-local.yml` và `application-dev.yml`.
- System property `io.netty.leakDetection.level` đặt tường minh luôn thắng config. Unit test và IT chạy `paranoid`.

**JVM flags đã duyệt** (trả lời "duyệt đề xuất JVM flags"). Jib `container.jvmFlags` của gateway, đúng thứ tự sau:
```
-XX:+UseZGC -XX:+ZGenerational
-XX:InitialRAMPercentage=50 -XX:MaxRAMPercentage=50 -XX:+AlwaysPreTouch
-XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp
--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
-Dio.netty.tryReflectionSetAccessible=true
-Djava.security.egd=file:/dev/urandom -Dfile.encoding=UTF-8
```
- `-XX:MaxDirectMemorySize` **không** nằm trong image. Deployment đặt nó qua `JAVA_TOOL_OPTIONS`, bằng 25% bộ nhớ container.
- Profile `prod` mà không đặt `MaxDirectMemorySize` thì gateway không start: mặc định của JVM là bằng max heap (50%), cộng với heap thành 100% bộ nhớ container.
- Dev compose giữ `JAVA_TOOL_OPTIONS: "-Xmx1g -XX:MaxDirectMemorySize=1g"`. `-Xmx` tường minh thắng `MaxRAMPercentage`.

**Fuzz** (spec §11): 10 phút cho mỗi target, không có finding, `LeakWatch.leaks() == 0` ở leak level `paranoid`.

**Version** (spec §2): mọi module `0.1.0`, không SNAPSHOT trên `main`. Tag `v0.1.0` chỉ tạo trên `main` sau khi merge, và chỉ khi người dùng đồng ý (push tag là thao tác ra ngoài).

**Ngoài phạm vi:** Helm chart; publish lên GitHub Packages (spec §2 "sau M3"); bài 50k session (TODO spec §14.6); benchmark `AckBatcher` (spec §6.3 chỉ đổi queue khi cần, §11 không liệt kê).

## Review Focus

1. **Image chạy với `-Xmx1g` của dev.** `InitialRAMPercentage=50` trên máy có RAM lớn có thể vượt `-Xmx1g`. Kỳ vọng: JVM tự kẹp initial heap về max heap và start bình thường. Kiểm ở Task 2 bằng `make up` (healthy) và một `docker run --memory=512m` không có `-Xmx`.
2. **Thiếu `MaxDirectMemorySize` ở prod.** Kỳ vọng: không start, thông báo rõ ràng do `FailureAnalyzer` in ra; local và dev không bị ảnh hưởng. Test ở Task 1.
3. **System property leak detection đặt tay.** `-Dio.netty.leakDetection.level=paranoid` phải thắng `gateway.netty.leak-detection=disabled`, nếu không IT sẽ âm thầm không dò leak. Test ở Task 1.
4. **Fuzz frame khi đã xác thực.** Sau khi xác thực, giới hạn là `frameMax`, không còn `helloMax`. Harness phải fuzz cả hai trạng thái (bit đầu tiên của input). Ở Task 3.
5. **Lệch version sau khi bump.** Makefile, script devenv và README còn chứa `0.1.0-SNAPSHOT` thì `make local-check` hỏng. `release-check.sh` bắt lỗi này (Task 8).

---

### Task 1: Leak detection theo profile và kiểm tra JVM lúc khởi động

**Files:**
- Create: `gateway-service/src/main/java/com/pangate/gateway/config/NettyLeakDetectionPostProcessor.java`
- Create: `gateway-service/src/main/java/com/pangate/gateway/diagnostics/JvmSettings.java`
- Create: `gateway-service/src/main/java/com/pangate/gateway/diagnostics/JvmConfigException.java`
- Create: `gateway-service/src/main/java/com/pangate/gateway/diagnostics/JvmConfigFailureAnalyzer.java`
- Modify: `gateway-service/src/main/resources/META-INF/spring.factories` (nối `NettyLeakDetectionPostProcessor` vào danh sách `EnvironmentPostProcessor` sau `DevSecretsEnvironmentPostProcessor`, và `JvmConfigFailureAnalyzer` vào danh sách `FailureAnalyzer`)
- Modify: `application.yml`, `application-local.yml`, `application-dev.yml`
- Modify: `gateway-service/pom.xml` (surefire và failsafe: `<systemPropertyVariables><io.netty.leakDetection.level>paranoid</…>`)
- Modify: `config/EdgeConfig.java` (bean `jvmSettingsCheck`) hoặc config lifecycle hiện có
- Test: `config/NettyLeakDetectionPostProcessorTest.java`, `diagnostics/JvmSettingsTest.java`

**Interfaces:**
- Produces:
  - `NettyLeakDetectionPostProcessor implements EnvironmentPostProcessor`. Nó đọc `gateway.netty.leak-detection`, mặc định `simple`, rồi gọi `ResourceLeakDetector.setLevel`, trừ khi `System.getProperty("io.netty.leakDetection.level") != null`.
  - `record JvmSettings(List<String> gcNames, long maxHeapBytes, long maxDirectBytes, boolean directExplicit, String leakLevel)` với:
    - `static JvmSettings read()`: dùng `GarbageCollectorMXBean`, `Runtime.maxMemory()`, và `HotSpotDiagnosticMXBean.getVMOption("MaxDirectMemorySize")` (`origin != DEFAULT`) để biết `MaxDirectMemorySize` có được đặt tường minh không.
    - `String describe()`: `"jvm gc=<names joined by ,> heapMax=<MiB>MiB directMax=<MiB|default> leak=<level>"`.
    - `void check(Set<String> activeProfiles)`: profile `prod` và `!directExplicit` → `throw new JvmConfigException("MaxDirectMemorySize")`.

- [ ] **Step 1: Viết test fail**
  - `NettyLeakDetectionPostProcessorTest`:
    - `configDisabledSetsDisabled`: property `disabled` → `ResourceLeakDetector.getLevel() == DISABLED`.
    - `explicitSystemPropertyWins`: system property `paranoid` cùng config `disabled` → level vẫn `PARANOID`.
    - Khôi phục level cũ trong `@AfterEach`.
  - `JvmSettingsTest`:
    - `prodWithoutDirectFails`: `new JvmSettings(List.of("G1"), 1<<30, 1<<30, false, "disabled").check(Set.of("prod"))` ném `JvmConfigException`.
    - `devWithoutDirectPasses`: tương tự với `Set.of("dev")` → không ném.
    - `describeFormat`: `describe()` bắt đầu bằng `"jvm gc=G1 heapMax=1024MiB directMax=default"`.

- [ ] **Step 2: Chạy, thấy fail**

Run: `./mvnw -f gateway-service test -Dtest='NettyLeakDetectionPostProcessorTest,JvmSettingsTest'`
Expected: lỗi biên dịch, class chưa tồn tại.

- [ ] **Step 3: Hiện thực**
  - Hiện thực các class ở Interfaces và đăng ký trong `spring.factories`.
  - Bean `jvmSettingsCheck` (chạy một lần khi start) gọi `check(env.getActiveProfiles())` rồi `log.info(describe())`.
  - Analyzer in ra: `"-XX:MaxDirectMemorySize is not set; with profile prod set it in JAVA_TOOL_OPTIONS (25% of container memory)"`.
  - Thêm `gateway.netty.leak-detection` vào ba file yml theo Global Constraints.

- [ ] **Step 4: Chạy lại, thấy pass; chạy cả suite**

Run: `./mvnw -f gateway-service verify`
Expected: unit và IT xanh. Đếm `grep -c 'LEAK:'` trong `target/surefire-reports/*-output.txt` và `target/failsafe-reports/*-output.txt`. Khác 0 là một finding: sửa leak đó, hoặc ghi `Ruling:` vào ledger nếu leak nằm trong code test.

- [ ] **Step 5: Commit** `feat(gateway): leak detection level by profile; fail fast on prod without MaxDirectMemorySize`

### Task 2: Image prod với JVM flags

**Files:**
- Modify: `gateway-service/pom.xml` (jib: `container.jvmFlags` theo Global Constraints; `to.tags` thêm `${project.version}`)
- Modify: `gateway-service/README.md` (mục "Production image": bảng flag ở trên, `JAVA_TOOL_OPTIONS` cần đặt, yêu cầu container phải có memory limit vì `AlwaysPreTouch` chạm 50% bộ nhớ)
- Modify: spec §9.1 (dòng "JVM" trong bảng profile ghi flag thực tế), §10 (dòng OOM)

**Interfaces:**
- Consumes: dòng log `jvm gc=…` của Task 1.

- [ ] **Step 1: Kiểm chứng fail trước khi sửa**

Run: `make images && make up && docker compose -f platform-devenv/docker-compose.yml logs gateway | grep 'jvm gc='`
Expected: thấy `gc=G1…`, chưa phải ZGC. Sau đó `make down`.

- [ ] **Step 2: Thêm `jvmFlags` và tag version vào jib**

- [ ] **Step 3: Kiểm chứng trong devenv**
  - Run: `make images up smoke`, rồi `docker compose … logs gateway | grep 'jvm gc='`.
    Expected: mọi container healthy; smoke OK; dòng log có `gc=ZGC`, `heapMax=1024MiB`, `directMax=1024MiB`.
  - Run: `docker inspect pangate/gateway-service:dev --format '{{json .Config.Entrypoint}}'`.
    Expected: chứa `-XX:+ZGenerational` và `-XX:+ExitOnOutOfMemoryError`, không chứa `MaxDirectMemorySize`.
  - Run: `docker run --rm --memory=512m --entrypoint java pangate/gateway-service:dev -XX:+UseZGC -XX:+ZGenerational -XX:InitialRAMPercentage=50 -XX:MaxRAMPercentage=50 -XX:+AlwaysPreTouch -XX:+PrintFlagsFinal -version | grep -E ' MaxHeapSize'`.
    Expected: khoảng 256 MiB (Review Focus 1).
  - Run: `docker run --rm -e SPRING_PROFILES_ACTIVE=prod pangate/gateway-service:dev`.
    Expected: exit khác 0, có thông báo `MaxDirectMemorySize is not set` (Review Focus 2).
  - Cuối cùng: `make down`.

- [ ] **Step 4: Commit** `feat(gateway): production JVM flags in the Jib image`

### Task 3: `LeakWatch` + Jazzer + fuzz `FrameCodec`

**Files:**
- Modify: `gateway-service/pom.xml` (dependency `jazzer-junit` scope test)
- Create: `gateway-service/src/test/java/com/pangate/gateway/test/LeakWatch.java`
- Create: `gateway-service/src/test/java/com/pangate/gateway/test/LeakWatchTest.java`
- Create: `gateway-service/src/test/java/com/pangate/gateway/protocol/FrameCodecFuzzTest.java`
- Create: seed trong `gateway-service/src/test/resources/com/pangate/gateway/protocol/FrameCodecFuzzTestInputs/decode/` (4 file: HELLO hợp lệ, PING, UPSTREAM, byte rỗng)

**Interfaces:**
- Produces `LeakWatch` (dùng ở Task 4):
  - `static void install()`: gắn một Logback appender vào logger `io.netty.util.ResourceLeakDetector` và đếm event ERROR có message chứa `"LEAK:"`. Idempotent.
  - `static int leaks()`.
  - `static void reset()`.
  - `static void collect()`: 5 vòng `System.gc()` + cấp phát rồi release 1000 `PooledByteBufAllocator.DEFAULT.buffer(64)`, để detector báo các leak đang chờ.

- [ ] **Step 1: Test fail cho `LeakWatch`**

`LeakWatchTest.detectsDroppedBuffer`:
- Đặt `ResourceLeakDetector.setLevel(PARANOID)`, gọi `install()` và `reset()`.
- Cấp phát `ByteBuf` rồi bỏ tham chiếu mà không release.
- Gọi `collect()`, assert `leaks() > 0`.
- Khôi phục level.

- [ ] **Step 2: Chạy, thấy fail** (`-Dtest=LeakWatchTest`). Expected: lỗi biên dịch.

- [ ] **Step 3: Hiện thực `LeakWatch`; chạy, thấy pass**

- [ ] **Step 4: Viết `FrameCodecFuzzTest.decode(FuzzedDataProvider data)`, `@FuzzTest(maxDuration = "10m")`**
  - `boolean authenticated = data.consumeBoolean()` (Review Focus 4). Nếu true, đặt `ChannelAttrs.AUTHENTICATED` trên `EmbeddedChannel`.
  - Pipeline: `new FrameCodec(TestLimits.holder())` cùng một handler cuối, ghi lại `Frame` hoặc exception.
  - `writeInbound(Unpooled.wrappedBuffer(data.consumeRemainingAsBytes()))`.
  - Invariant:
    - Lỗi duy nhất được phép là `ProtocolException` với `CloseCode.BAD_FRAME`.
    - Frame đi ra có type thuộc `CLIENT_TYPES`.
    - `finishAndReleaseAll()`.
  - `@BeforeAll`: đặt `PARANOID`, gọi `LeakWatch.install()` và `reset()`. `@AfterAll`: gọi `collect()`, assert `leaks() == 0`.

- [ ] **Step 5: Chạy regression mode**

Run: `./mvnw -f gateway-service test -Dtest=FrameCodecFuzzTest`
Expected: PASS với 4 seed.

Kiểm thêm rằng invariant có tác dụng: tạm đổi dòng `"malformed frame"` trong `FrameCodec` để ném `IllegalStateException`. Kỳ vọng FAIL, rồi hoàn tác thay đổi.

- [ ] **Step 6: Commit** `test(gateway): Jazzer fuzz for FrameCodec with ByteBuf leak watch`

### Task 4: Fuzz `HelloAuthHandler`

**Files:**
- Create: `gateway-service/src/test/java/com/pangate/gateway/pipeline/HelloAuthFuzzTest.java`
- Create: seed trong `…/pipeline/HelloAuthFuzzTestInputs/hello/` (3 file, một cho mỗi mode bên dưới)

**Interfaces:**
- Consumes: `LeakWatch` (Task 3). Pipeline giống `@BeforeEach` của `HelloAuthHandlerTest`: `FrameCodec → HelloAuthHandler → SessionHandler → ErrorTerminator`, dùng `LoopFixture` và `AuthFixture` mới cho mỗi input.

- [ ] **Step 1: Viết `hello(FuzzedDataProvider data)`, `@FuzzTest(maxDuration = "10m")`**

`mode = data.consumeInt(0, 3)` quyết định input:

| mode | Input |
|---|---|
| 0 | Byte ngẫu nhiên (`consumeRemainingAsBytes`) |
| 1 | `Hello` có `jwt` là chuỗi ngẫu nhiên |
| 2 | JWT hợp lệ do `AuthFixture` ký, rồi lật 1 byte ở vị trí `consumeInt` |
| 3 | `Hello` có `ticket` ngẫu nhiên, cùng `device_id`, `sdk_version`, `platform` ngẫu nhiên |

Invariant sau `writeInbound` và khi mọi task đang chờ của loop đã chạy xong. Đúng **một** trong hai trường hợp:
- channel đã đóng;
- session `ACTIVE` và outbound có đúng một frame `CONNECTED`.

Không exception nào lọt ra `EmbeddedChannel.checkException()`. Cuối mỗi input gọi `finishAndReleaseAll()`. Leak check `@BeforeAll`/`@AfterAll` như Task 3.

- [ ] **Step 2: Chạy regression mode**

Run: `./mvnw -f gateway-service test -Dtest=HelloAuthFuzzTest`
Expected: PASS. Seed mode 2 với vị trí lật byte nằm trong chữ ký thì kết thúc bằng channel đóng.

Kiểm thêm rằng invariant có tác dụng: tạm bỏ việc ghi `CONNECTED` trong `HelloAuthHandler`. Kỳ vọng FAIL, rồi hoàn tác.

- [ ] **Step 3: Commit** `test(gateway): Jazzer fuzz for HelloAuthHandler`

### Task 5: Chạy fuzz 10 phút, `make fuzz`, CI hằng đêm

**Files:**
- Modify: `Makefile`. Target `fuzz`, với `FUZZ ?= FrameCodecFuzzTest` và lệnh `JAZZER_FUZZ=1 $(MVNW) -f gateway-service/pom.xml test -Dtest=$(FUZZ) -Dsurefire.failIfNoSpecifiedTests=false`.
- Create: `.github/workflows/fuzz.yml`. Chạy theo `workflow_dispatch` và `schedule: cron "17 19 * * *"`, matrix `FrameCodecFuzzTest` / `HelloAuthFuzzTest`, timeout 20 phút. Khi fail thì upload `gateway-service/src/test/resources/**/*Inputs/**`.
- Modify: `gateway-service/README.md` (mục "Fuzz").

- [ ] **Step 1: Chạy fuzz thật cho từng target**
  - Run: `make fuzz FUZZ=FrameCodecFuzzTest`, rồi `make fuzz FUZZ=HelloAuthFuzzTest`.
  - Expected: mỗi lần chạy khoảng 10 phút, `BUILD SUCCESS`, không có file crash mới trong thư mục `*Inputs`, `leaks() == 0`.
  - Nếu có finding, theo superpowers:systematic-debugging:
    - Giữ input crash làm seed regression; nó phải FAIL trước khi sửa.
    - Sửa code gateway, rồi chạy lại 10 phút.
  - Ghi số lần chạy (exec/s, tổng số exec) vào bảng kết quả của plan.

- [ ] **Step 2: Commit** `ci(gateway): make fuzz and nightly fuzz workflow`, kèm mọi seed mới.

### Task 6: Benchmark JMH (`-Pjmh`)

**Files:**
- Modify: `gateway-service/pom.xml`. Profile `jmh` gồm:
  - `build-helper` `add-test-source` cho `src/jmh/java`;
  - `jmh-core`, `jmh-generator-annprocess` scope test, cùng `annotationProcessorPaths`;
  - `exec-maven-plugin` `exec:exec` chạy `java -cp %classpath org.openjdk.jmh.Main ${jmh.args}` với `classpathScope=test`.
- Create: `gateway-service/src/jmh/java/com/pangate/gateway/protocol/FrameCodecBench.java`
  - `decodePush` và `encodePush` trên frame UPSTREAM/PUSH có payload 256 B.
  - Gọi `decode`/`encode` trực tiếp (cùng package, nên truy cập được method `protected`), với `ctx` lấy từ `EmbeddedChannel`.
  - Release buffer trong benchmark.
- Create: `gateway-service/src/jmh/java/com/pangate/gateway/session/PrincipalIndexBench.java`
  - `@Param({"100000"})` principal.
  - Benchmark `get` và cặp `addRemove`.
- Create: `gateway-service/src/jmh/java/com/pangate/gateway/loop/DeadlineWheelBench.java`
  - Wheel 100k node, deadline rải đều trên 60 s.
  - `tick` = `advanceTo(now + 1)` rồi lên lịch lại các node đến hạn.
- Modify: `Makefile`. Target `bench`, với `JMH_ARGS ?= -f 1 -wi 3 -i 5 -rf json -rff target/jmh.json`.
- Modify: `gateway-service/README.md` (mục "Benchmarks" có bảng baseline).

- [ ] **Step 1: Chạy profile khi chưa có benchmark**

Run: `./mvnw -f gateway-service -Pjmh test-compile exec:exec -Djmh.args="-l"`
Expected: FAIL. Profile chưa tồn tại hoặc danh sách benchmark rỗng.

- [ ] **Step 2: Thêm profile và 3 class benchmark**

- [ ] **Step 3: Smoke**

Run: `./mvnw -f gateway-service -Pjmh test-compile exec:exec -Djmh.args="-l"`
Expected: liệt kê đủ 5 benchmark: `decodePush`, `encodePush`, `get`, `addRemove`, `tick`.

- [ ] **Step 4: Đo baseline**

Run: `make bench`
Expected: `target/jmh.json` có 5 kết quả. Chép score và error vào README.

Spec không đặt ngưỡng. Nếu `tick` trên 100k node vượt 1 ms/op thì đó là finding: ghi `Ruling:` kèm số đo, không sửa trong plan này.

- [ ] **Step 5: Commit** `perf(gateway): JMH benchmarks for FrameCodec, PrincipalIndex, DeadlineWheel`

### Task 7: Sửa test flaky `TenantCapBorrowerIT` khi teardown

Triệu chứng đã thấy ở M3b: lỗi một lần, khi BlockHound bắt Lettuce shutdown trong `RedisFixture.close()`.

**Files:**
- Modify: `gateway-service/src/it/java/com/pangate/gateway/registry/RedisFixture.java` (và `BlockHoundExtension` nếu nguyên nhân nằm ở đó)

- [ ] **Step 1: Tái hiện**

Run: chạy `TenantCapBorrowerIT` 15 lần liên tiếp (vòng lặp shell quanh lệnh IT ở M3b), đếm số lần lỗi và giữ stack trace của BlockHound.
Expected: ít nhất 1 lần lỗi.

Nếu 15 lần đều xanh, chạy 30 lần. Vẫn xanh thì ghi `Ruling:` "không tái hiện được", rồi sửa theo stack trace đã lưu trong log M3b (`/tmp/claude-0/it-edge.log`, nếu còn).

- [ ] **Step 2: Sửa theo nguyên nhân gốc** (superpowers:systematic-debugging)

Hướng dự kiến: `client.shutdown()` chạy trên thread event loop của Lettuce, vốn không thuộc `gw-io-*`. Nếu BlockHound bắt một thread `gw-io-*` thì cần tìm callback Lettuce nào chạy trên loop của gateway.

Không được tắt BlockHound cho cả IT.

- [ ] **Step 3: Kiểm chứng**

Run: cùng vòng lặp 15 lần.
Expected: 15/15 xanh.

- [ ] **Step 4: Commit** `test(gateway): stable RedisFixture teardown under BlockHound`

### Task 8: ADR, version 0.1.0, release check

**Files:**
- Create: `docs/adr/README.md` (bảng mục lục) và `docs/adr/0001-…md` → `0012-…md`.
  - 0001–0010 đúng tên và nguồn ở spec §13.
  - 0011: "OTel span qua `LoopSafeSpanProcessor` thay vì `BatchSpanProcessor`" (lý do: `BatchSpanProcessor` dùng `ArrayBlockingQueue`, park event loop; M3b).
  - 0012: "`tenantconns:{t}` là hash tổng theo node, hạn 30 s" (spec §14.11, ruling R2).
  - Mỗi ADR có đúng 5 heading: `## Trạng thái` (Accepted, ngày), `## Bối cảnh`, `## Quyết định`, `## Hệ quả`, `## Nguồn`.
- Create: `platform-devenv/scripts/release-check.sh <expected-version>`. Fail (exit 1, in dòng lỗi) khi:
  - một trong 5 `pom.xml` có `<version>` của project hoặc parent khác `<expected-version>`;
  - `grep -rn 'SNAPSHOT'` trong `Makefile`, `platform-devenv/`, `*/README.md`, `.github/` có kết quả;
  - một file `docs/adr/0*.md` thiếu trong `docs/adr/README.md` hoặc thiếu một trong 5 heading.
- Modify: 5 `pom.xml` (`0.1.0-SNAPSHOT` → `0.1.0`).
- Modify: `Makefile`. Thêm `VERSION := 0.1.0`; `GATEWAY_JAR` và `DEVTOOLS_JAR` dùng `$(VERSION)`; target `release-check`.
- Modify: README và script devenv còn chứa version.
- Modify: `.github/workflows/ci.yml`. Job `build` chạy `make release-check` trước `make verify`.
- Create: `CHANGELOG.md` mục `0.1.0`, tóm tắt M1, M2a, M2b, M3a, M3b, M3c, một dòng cho mỗi tính năng, kèm các TODO còn mở (50k session, các câu hỏi mở của spec §14).
- Modify: spec §12. Ghi "M3: hoàn thành" kèm số đo, và đánh dấu §13 đã ghi.

- [ ] **Step 1: Viết `release-check.sh`, chạy, thấy fail**

Run: `make release-check`
Expected: FAIL, liệt kê các pom còn `0.1.0-SNAPSHOT` và các ADR còn thiếu.

- [ ] **Step 2: Viết 12 ADR và README index**
  - Mỗi ADR một trang, nội dung lấy từ spec, không thêm quyết định mới.
  - Run: `make release-check`. Expected: chỉ còn lỗi version.

- [ ] **Step 3: Bump version, sửa Makefile, README và script, viết CHANGELOG**

Run: `make release-check`
Expected: `release-check OK 0.1.0`.

- [ ] **Step 4: Kiểm chứng toàn bộ trên version mới**

Run lần lượt, chờ khoảng 60 s trước `load-check` (TIME_WAIT):
- `make bootstrap && make verify`
- `make local-check`
- `make memory-check`
- `make load-check`
- `make images up smoke down`

Expected: tất cả xanh. Heap/session < 5120 B, lag p99 < 20 ms. Ghi số đo vào bảng kết quả bên dưới.

- [ ] **Step 5: Commit** `docs: ADR 0001–0012`, rồi `chore(release): 0.1.0`. Push nhánh.

Tag `v0.1.0` **không** tạo ở bước này. Tag được đặt trên `main` sau khi merge, khi người dùng đồng ý.

---

## Kết quả (điền khi thực hiện)

| Hạng mục | Kết quả |
|---|---|
| Fuzz `FrameCodec` 10 phút | Sau final review (limit 256 B / 1 KiB, input pooled): 12.054.022 lần chạy trong 601 s (~20k/s), cov 535, corpus 510; không finding, không leak. Lần đầu (harness yếu hơn): 22,65 triệu |
| Fuzz `HelloAuthHandler` 10 phút | Sau final review (bắt close 4500, input pooled): 4.293.939 lần chạy trong 601 s (~7,1k/s), cov 1652, corpus 678; không finding, không leak. Lần đầu: 5,58 triệu |
| JMH `decodeUpstream` / `encodePush` / `get` / `addRemove` / `tick` | 217,6 ns / 139,2 ns / 55,7 ns / 192,5 ns / 13,7 µs |
| `TenantCapBorrowerIT` 15 lần liên tiếp | 15/15 trước và sau khi sửa; cơ chế tái hiện tất định bằng `SharedLockBlockHoundTest` |
| Unit / IT | gateway 561 unit + 97 IT (sau final review); contracts 11, starter 14, devtools 31 |
| Heap/session, lag p99 | 4808 B (10k session); 18k session, lag p99 1,68 ms, shed về level 0 (2 lần chuyển level lúc ramp) |
| `make images up smoke` (log `jvm gc=ZGC`) | Xanh; `jvm gc=ZGC Minor Cycles,… heapMax=1024MiB directMax=1024MiB leak=simple` |
| Leak detection `paranoid` trên cả suite | 46 leak `SslContextProvider` (production, đã sửa bằng `close()`); 21 leak còn lại tạo trong code test |
