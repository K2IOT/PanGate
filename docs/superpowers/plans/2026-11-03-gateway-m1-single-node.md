# Gateway M1 (Single-node) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dựng base project (parent, contracts, starter, devtools, devenv) và gateway-service single-node: giữ kết nối WSS/TCP qua TLS, xác thực `HELLO` bằng JWT hoặc resume ticket, heartbeat bằng `DeadlineWheel`, admission cơ bản, cổng quản trị; chạy được ở local (IDE) và dev (Docker compose).

**Architecture:** Raw Netty 4.2 trên Spring Boot 3.5 (Spring chỉ lo lifecycle và config). State shared-nothing theo event loop (`LoopContext`); một `DeadlineWheel` mỗi loop xử lý mọi deadline. Chỉ owner loop được sửa `Session`. Mọi việc bất đồng bộ (refresh JWKS) quay về loop qua `eventLoop().execute`.

**Tech Stack:** Java 21, Spring Boot 3.5.16, Netty 4.2.20.Final + netty-tcnative-boringssl-static, protobuf-java 4.36.2, nimbus-jose-jwt 10.10, BouncyCastle, fastutil, picocli, Micrometer Prometheus, JUnit 5 / AssertJ / Awaitility, BlockHound, Jib, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-11-03-gateway-service-design.md` (gọi tắt "spec", mục `§x.y`). Executor đọc spec cùng plan. Plan này chỉ hiện thực phần **M1** của spec §12; M2 (Redis, Kafka, PushStream) và M3 (hardening) có plan riêng.

## Global Constraints

- Java 21 (`maven.compiler.release=21`); Maven 3.9.11 qua Maven Wrapper ở gốc repo, mọi lệnh dùng `./mvnw -f <module>`.
- `platform-parent` có parent là `org.springframework.boot:spring-boot-starter-parent:3.5.16` và ghi đè `netty.version=4.2.20.Final`. tcnative lấy theo netty-bom (`2.0.85.Final`), không pin riêng.
- Version pin trong `platform-parent`: `protobuf-java 4.36.2`, `io.github.ascopes:protobuf-maven-plugin 5.1.12` (`<protoc>4.36.2</protoc>`), `nimbus-jose-jwt 10.10`, `bcpkix-jdk18on 1.86`, `fastutil 8.5.19`, `picocli 4.7.7`, `blockhound 1.0.18.RELEASE`, `jib-maven-plugin 3.5.2`. Không dùng SNAPSHOT của bên thứ ba và không dùng `latest`.
- `groupId=com.pangate`, `version=0.1.0-SNAPSHOT`. Package gốc: `com.pangate.contracts.v1`, `com.pangate.starter`, `com.pangate.devtools`, `com.pangate.gateway`.
- Thứ tự build: parent → contracts → starter → devtools → gateway. gateway-service chỉ phụ thuộc devtools ở scope `test`.
- gateway-service dùng `spring-boot-starter-web` (Spring MVC + Tomcat) **chỉ** cho cổng quản trị. **Không** được kéo `spring-boot-starter-webflux` hay `io.projectreactor.netty:*` (enforcer `bannedDependencies`), vì Reactor Netty của Boot 3.5 cần Netty 4.1.
- Phase lifecycle (spec §3.7): Tomcat của Spring Boot là `MAX − 2048`; `NettyGatewayServer` là `Integer.MAX_VALUE − 100`. Mọi `SmartLifecycle` của gateway phải có phase > `MAX − 2048`, để cổng quản trị sống suốt quá trình khởi động và drain.
- Port: WSS `8443` path `/v1/ws` subprotocol `pangate.v1`; TCP+TLS `8883` với ALPN `pangate.v1`; management `8081` (`server.port`, Tomcat).
- Giới hạn prod (spec §9): hard-cap 700000, soft-cap 550000, per-IP 200, frame 65536 B, HELLO 8192 B, HELLO timeout 5 s, handshake timeout 10 s, heartbeat 30 s (idle timeout = 3 × heartbeat = 90 s), session TTL 24 h, 20 handshake/phút/principal, admission 5000/s burst 10000, ramp từ 500/s trong 2 phút.
- Giới hạn profile `local`: hard-cap 100000, soft-cap 80000, per-IP 100000. Profile `dev`: hard-cap 200000.
- Close code đúng bảng spec §4.8 (1000, 1001, 4000, 4001, 4002, 4003, 4004, 4008, 4009, 4010, 4011, 4012, 4013, 4500).
- Không có lời gọi blocking trên thread `gw-io-*`. BlockHound bật trong mọi test của gateway-service (surefire và failsafe chạy với `-XX:+AllowRedefinitionToAddDeleteMethods`).
- Dev secret (`DevSecretsEnvironmentPostProcessor`) chỉ chạy ở profile `local` hoặc `dev`; bật ở profile `prod` thì exit.
- Bố cục thư mục secret (`gateway.dev.dir`, local mặc định `${user.home}/.pangate-dev`, dev là `/dev-secrets`): `ca.crt`, `ca.key`, `gateway.crt`, `gateway.key` (PKCS#8 PEM), `es256.pem` (SEC1 PEM `BEGIN EC PRIVATE KEY`), `jwks.json`, `ticket-keys/{prev,current,next}` (một dòng `"<kid> <base64 32 byte>"`, kid lần lượt 1, 2, 3, ký bằng `current`), `principal-hash.key` (base64 32 byte).
- `kid` của key ES256 = JWK thumbprint RFC 7638 (`ECKey.computeThumbprint().toString()`), giống nhau ở gateway và devtools.
- Commit sau mỗi task theo Conventional Commits.

## Review Focus

1. **Client gửi nối tiếp `HELLO` rồi `PING` ngay, không chờ `CONNECTED`, trong lúc JWKS đang refresh vì `kid` lạ** → phải nhận `CONNECTED` rồi `PONG`, không bị đóng 4000 (`autoRead=false` trong lúc chờ). Test: Task 16 `pipelinedFramesAfterHelloAreProcessedAfterConnected`, Task 18 `pipelinedHelloAndPingWithRotatedKey`.
2. **Client ngắt đột ngột** (đóng TCP trước TLS, giữa upgrade, hoặc sau `CONNECTED` mà không gửi close frame) → các bộ đếm `activeCount` của loop, `IpConnectionCounter` và `PrincipalIndex` phải về 0, không rò rỉ. Test: Task 18 `abruptDisconnectsLeaveNoTrackedState`.
3. **Lệch đồng hồ giữa issuer và gateway** → JWT có `exp` hoặc `nbf` lệch ≤ 30 s vẫn được chấp nhận, lệch > 30 s thì 4001. Test: Task 12 `clockSkewWithin30sAccepted`, `clockSkewBeyond30sRejected`.
4. **Gateway restart rồi client reconnect bằng resume ticket cũ** (key nằm trong `~/.pangate-dev`), kể cả ticket ký bằng key `prev` → vẫn hợp lệ. Restart ngay trên cùng port không lỗi bind. Test: Task 11 `verifiesTicketSignedWithPreviousKey`, Task 18 `restartKeepsTicketsValidAndRebindsImmediately`.
5. **Địa chỉ IPv6 và IPv4-mapped IPv6** khi đếm per-IP → các địa chỉ cùng /64 dùng chung một bộ đếm; `::ffff:10.0.0.1` tính như `10.0.0.1`. Test: Task 13 `ipv6SameSlash64SharesCounter`, `ipv4MappedIpv6CountsAsIpv4`.

---

## File Structure

```
PanGate/
├── .gitignore  Makefile  mvnw  .mvn/wrapper/maven-wrapper.properties  README.md
├── .github/workflows/ci.yml
├── platform-parent/pom.xml
├── platform-contracts/
│   ├── pom.xml  buf.yaml
│   ├── proto/pangate/v1/frame.proto
│   └── src/test/java/com/pangate/contracts/v1/FrameRoundTripTest.java
├── platform-spring-starter/
│   ├── pom.xml
│   └── src/main/java/com/pangate/starter/
│       ├── secrets/   SecretSource, FileSecretSource, SecretNotFoundException
│       ├── logging/   PrincipalHasher
│       └── autoconfigure/ PangateProperties, PangateSecretsAutoConfiguration, PangateObservabilityAutoConfiguration
├── gateway-devtools/
│   ├── pom.xml
│   └── src/main/java/com/pangate/devtools/
│       ├── Main.java
│       ├── keys/DevKeys.java   jwt/JwtMinter.java  jwt/TokenRequest.java
│       ├── issuer/IssuerServer.java
│       ├── client/  GatewayClient, ClientTls, Hellos, CloseInfo, GatewayClosedException
│       └── cmd/     IssuerCommand, TokenCommand, ClientCommand, SmokeCommand, HealthCommand
├── gateway-service/
│   ├── pom.xml
│   ├── src/main/resources/ application.yml, application-local.yml, application-dev.yml, META-INF/spring.factories
│   └── src/main/java/com/pangate/gateway/
│       ├── GatewayApplication.java
│       ├── config/     GatewayProperties, GatewayLimits, LimitsHolder, GatewayConfig
│       ├── dev/        DevSecretsEnvironmentPostProcessor, DevSecretsGenerator
│       ├── diagnostics/ ListenerBindException, ListenerBindFailureAnalyzer, SecretNotFoundFailureAnalyzer, JwksConfigFailureAnalyzer
│       ├── loop/       WheelNode, DeadlineWheel, LoopContext, LoopRegistry
│       ├── server/     IoGroups, Transport, TickScheduler, NettyGatewayServer, NodeState
│       ├── protocol/   CloseCode, ProtocolException, ChannelCloser, ChannelAttrs, Frames, FrameCodec
│       ├── auth/       TicketKey, TicketKeyRing, ResumeTicket, ResumeTicketService, TicketVerifyResult,
│       │               JwksSource, HttpJwksSource, FileJwksSource, JwksSources, JwksConfigException, JwksCache,
│       │               ConnectClaims, JwtResult, JwtVerifier, PrincipalHandshakeLimiter
│       ├── shed/       TokenBucket, AdmissionController, IpConnectionCounter
│       ├── session/    ConnId, ConnRef, Session, SessionState, PrincipalIndex, SessionTracker
│       ├── pipeline/   AcceptGate, UpgradeGuard, WsBinaryAdapter, HelloAuthHandler, SessionHandler,
│       │               ErrorTerminator, WsChannelInitializer, TcpChannelInitializer, SslContextProvider
│       ├── metrics/    GatewayMetrics, EventLoopLagProbe
│       ├── mgmt/       ReadinessCheck, GatewayReadinessHealthIndicator, EventLoopLagHealthIndicator, AdminController
│       └── tools/      HealthCheck
│   ├── src/test/java/...   (unit, EmbeddedChannel)
│   └── src/it/java/com/pangate/gateway/it/  GatewayLocalIT
└── platform-devenv/
    ├── docker-compose.yml  README.md
    └── scripts/  local-start-check.sh  idle-memory.sh
```

---

### Task 1: Repo bootstrap và platform-parent

**Files:**
- Create: `.gitignore`, `README.md`, `mvnw`, `mvnw.cmd`, `.mvn/wrapper/maven-wrapper.properties`, `platform-parent/pom.xml`, `Makefile`

**Interfaces:**
- Produces: artifact `com.pangate:platform-parent:0.1.0-SNAPSHOT` (packaging `pom`) để mọi module dùng làm `<parent>`; target `make bootstrap`.

- [ ] **Step 1: Sinh Maven Wrapper ở gốc repo**

Run: `mvn -q -N wrapper:wrapper -Dmaven=3.9.11`
Expected: có `mvnw`, `.mvn/wrapper/maven-wrapper.properties` chứa `apache-maven/3.9.11`.

- [ ] **Step 2: Viết `platform-parent/pom.xml`**

- `<parent>` là `spring-boot-starter-parent:3.5.16`; `<properties>`: `java.version=21`, `netty.version=4.2.20.Final`, cùng property version cho từng thư viện ở Global Constraints.
- `dependencyManagement`: `protobuf-java`, `nimbus-jose-jwt`, `bcpkix-jdk18on`, `fastutil`, `picocli`, `blockhound`, cùng ba artifact nội bộ (`platform-contracts`, `platform-spring-starter`, `gateway-devtools`) ở `${project.version}`.
- `pluginManagement`: `protobuf-maven-plugin`, `jib-maven-plugin`. `maven-enforcer-plugin` với `requireJavaVersion [21,)` và `requireMavenVersion [3.9,)`; `bannedDependencies` để module gateway tự cấu hình.

- [ ] **Step 3: `.gitignore`, `README.md`, `Makefile`**

- `.gitignore`: `target/`, `.idea/`, `*.iml`, `.vscode/`, `.pangate-dev/`, `.gitnexus/`.
- `Makefile`: biến `MODULES := platform-parent platform-contracts platform-spring-starter gateway-devtools gateway-service`. Target `bootstrap` chạy `./mvnw -f $$m install -DskipTests` theo thứ tự đó, bỏ qua module chưa tồn tại (`[ -f $$m/pom.xml ] || continue`). Target `verify` giống `bootstrap` nhưng chạy `install` có test và IT. Các task sau bổ sung thêm target.
- `README.md`: 10–15 dòng gồm yêu cầu (JDK 21, Docker cho dev), `make bootstrap`, và link tới spec và `platform-devenv/README.md`.

- [ ] **Step 4: Kiểm tra**

Run: `make bootstrap`
Expected: `BUILD SUCCESS` cho `platform-parent`; `~/.m2/repository/com/pangate/platform-parent/0.1.0-SNAPSHOT/` tồn tại.

- [ ] **Step 5: Commit**

```bash
git add .gitignore README.md Makefile mvnw mvnw.cmd .mvn platform-parent
git commit -m "build: add maven wrapper, platform-parent and Makefile bootstrap"
```

---

### Task 2: platform-contracts — `frame.proto`

**Files:**
- Create: `platform-contracts/pom.xml`, `platform-contracts/buf.yaml`, `platform-contracts/proto/pangate/v1/frame.proto`
- Test: `platform-contracts/src/test/java/com/pangate/contracts/v1/FrameRoundTripTest.java`

**Interfaces:**
- Produces (Java, `com.pangate.contracts.v1`, `java_multiple_files=true`): `Frame`, `FrameType`, `DeliveryClass`, `Hello`, `Connected`, `Ack`, `Reauth`, `Reconnect`, `ErrorInfo`, `Subscribe`.
- `FrameType`: `FRAME_TYPE_UNSPECIFIED=0, FRAME_TYPE_HELLO=1, FRAME_TYPE_CONNECTED=2, FRAME_TYPE_PING=3, FRAME_TYPE_PONG=4, FRAME_TYPE_PUSH=5, FRAME_TYPE_ACK=6, FRAME_TYPE_UPSTREAM=7, FRAME_TYPE_UPSTREAM_ACK=8, FRAME_TYPE_SUBSCRIBE=9, FRAME_TYPE_UNSUBSCRIBE=10, FRAME_TYPE_RPC_REQ=11, FRAME_TYPE_RPC_RES=12, FRAME_TYPE_REAUTH=13, FRAME_TYPE_RECONNECT=14, FRAME_TYPE_ERROR=15`.
- `DeliveryClass`: `DELIVERY_CLASS_UNSPECIFIED=0, DELIVERY_CLASS_EPHEMERAL=1, DELIVERY_CLASS_RELIABLE=2, DELIVERY_CLASS_ORDERED=3`.
- `Frame` có field và số thứ tự đúng spec §3.3 của tài liệu gốc (`type=1 … topic_seq=11`). Các message payload có field đúng spec §4.5 của spec này (`ErrorInfo` thay cho `Error`).

- [ ] **Step 1: Viết test round-trip thất bại**

```java
@Test void helloRoundTripsInsideFramePayload() throws Exception {
  Hello hello = Hello.newBuilder().setJwt("t").setDeviceId("d1").setSdkVersion("1.0.0")
      .addCapabilities("zstd").build();
  Frame f = Frame.newBuilder().setType(FrameType.FRAME_TYPE_HELLO).setPayload(hello.toByteString()).build();
  Frame parsed = Frame.parseFrom(f.toByteArray());
  assertThat(parsed.getType()).isEqualTo(FrameType.FRAME_TYPE_HELLO);
  assertThat(Hello.parseFrom(parsed.getPayload())).isEqualTo(hello);
}
@Test void errorInfoCarriesFatalAndRetryAfter() { /* code=4013, retry_after_ms=15000, fatal=true round-trips */ }
@Test void connectedHasSeqUnknownField() { /* Connected.newBuilder().setSeqUnknown(true) round-trips */ }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f platform-contracts test`
Expected: lỗi compile `cannot find symbol: class Hello`.

- [ ] **Step 3: Viết `frame.proto`, `buf.yaml`, `pom.xml`**

- `frame.proto`: `syntax="proto3"; package pangate.v1; option java_package="com.pangate.contracts.v1"; option java_multiple_files=true;`.
- `buf.yaml`: `version: v2`, `modules: [{path: proto}]`, lint `STANDARD`.
- `pom.xml`: parent `platform-parent`; dependency `protobuf-java`; plugin `protobuf-maven-plugin` goal `generate` với `<protoc>4.36.2</protoc>` và `<sourceDirectories><sourceDirectory>${project.basedir}/proto</sourceDirectory></sourceDirectories>`; test dependency `spring-boot-starter-test`.

- [ ] **Step 4: Chạy lại để thấy pass**

Run: `./mvnw -f platform-contracts install`
Expected: `Tests run: 3, Failures: 0`, `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add platform-contracts
git commit -m "feat(contracts): add pangate.v1 frame protocol"
```

---

### Task 3: platform-spring-starter — secrets, principal hash, common tags

**Files:**
- Create: `platform-spring-starter/pom.xml`; `src/main/java/com/pangate/starter/secrets/{SecretSource,FileSecretSource,SecretNotFoundException}.java`; `logging/PrincipalHasher.java`; `autoconfigure/{PangateProperties,PangateSecretsAutoConfiguration,PangateObservabilityAutoConfiguration}.java`; `src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Test: `src/test/java/com/pangate/starter/secrets/FileSecretSourceTest.java`, `logging/PrincipalHasherTest.java`, `autoconfigure/PangateAutoConfigurationTest.java`

**Interfaces:**
- Produces:
  - `interface SecretSource { byte[] get(String name); String describe(String name); }`. `describe` trả mô tả vị trí (đường dẫn file) để in vào thông báo lỗi.
  - `final class FileSecretSource implements SecretSource { FileSecretSource(Path dir) }`
  - `final class SecretNotFoundException extends RuntimeException { String secretName(); String location(); }`
  - `final class PrincipalHasher { PrincipalHasher(byte[] key); String hash(String principalId); }`: HMAC-SHA256, 16 ký tự hex đầu.
  - `@ConfigurationProperties("pangate") record PangateProperties(String cell, String az, String node, String service, Secrets secrets)` với `record Secrets(String source, Path dir)`.
  - Bean `SecretSource` khi `pangate.secrets.source=file` (mặc định). Bean `PrincipalHasher` dùng secret `principal-hash.key` (base64), tạo **lazy** qua `ObjectProvider` để app khởi động được khi chưa có key.
  - Bean `MeterRegistryCustomizer<MeterRegistry>` gắn common tags `cell`, `az`, `node`, `service` (bỏ qua tag rỗng).

- [ ] **Step 1: Viết test thất bại**

```java
// FileSecretSourceTest
@Test void readsNestedSecret(@TempDir Path dir) { /* ticket-keys/current → bytes khớp nội dung file */ }
@Test void missingSecretThrowsWithPath(@TempDir Path dir) {
  assertThatThrownBy(() -> new FileSecretSource(dir).get("gateway.key"))
      .isInstanceOf(SecretNotFoundException.class)
      .hasMessageContaining("gateway.key").hasMessageContaining(dir.toString());
}
@Test void rejectsPathTraversal(@TempDir Path dir) { /* get("../x") → IllegalArgumentException */ }
// PrincipalHasherTest
@Test void deterministicSixteenHexChars() { /* cùng key + input → cùng chuỗi, khớp ^[0-9a-f]{16}$ */ }
@Test void differentKeysDifferentHashes() { }
// PangateAutoConfigurationTest (ApplicationContextRunner)
@Test void commonTagsApplied() { /* pangate.cell=cell-1, az=az-a → meter có tag cell=cell-1, az=az-a */ }
@Test void secretSourceUsesConfiguredDir(@TempDir Path dir) { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f platform-spring-starter test`
Expected: lỗi compile do thiếu class.

- [ ] **Step 3: Hiện thực các class và file `AutoConfiguration.imports`**

`pom.xml`: parent `platform-parent`; dependency `spring-boot-autoconfigure`, `micrometer-core`, `spring-boot-configuration-processor` (optional); test dependency `spring-boot-starter-test`.

- [ ] **Step 4: Chạy để thấy pass**

Run: `./mvnw -f platform-spring-starter install`
Expected: mọi test pass, `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add platform-spring-starter Makefile
git commit -m "feat(starter): file secret source, principal hasher, common metric tags"
```

---

### Task 4: gateway-devtools — keys, JWT minter, issuer, `token`, `health`

**Files:**
- Create: `gateway-devtools/pom.xml`; `src/main/java/com/pangate/devtools/Main.java`; `keys/DevKeys.java`; `jwt/{JwtMinter,TokenRequest}.java`; `issuer/IssuerServer.java`; `cmd/{IssuerCommand,TokenCommand,HealthCommand}.java`
- Test: `src/test/java/com/pangate/devtools/keys/DevKeysTest.java`, `jwt/JwtMinterTest.java`, `issuer/IssuerServerTest.java`

**Interfaces:**
- Produces:
  - `DevKeys.loadOrCreateEs256(Path pem) -> com.nimbusds.jose.jwk.ECKey`: nếu chưa có thì sinh P-256, ghi SEC1 PEM bằng `JcaPEMWriter.writeObject(KeyPair)` (ghi file tạm rồi `ATOMIC_MOVE`); `kid` = thumbprint. `DevKeys.writeJwks(ECKey key, Path file)` ghi JWKS public. `DevKeys.load(Path pem) -> ECKey` dùng `ECKey.parseFromPEMEncodedObjects`.
  - `record TokenRequest(String tenant, String principal, String device, List<String> topics, Duration ttl)`.
  - `JwtMinter(ECKey key, String issuer)`; `String mint(TokenRequest req, Instant now)`. Header `alg=ES256`, `kid`. Claims: `iss`, `aud="pangate-gw"`, `sub=principal`, `tenant`, `device`, `topics`, `jti` (UUID), `iat`, `nbf=now`, `exp=now+ttl`.
  - `IssuerServer(ECKey key, int port)` (JDK `HttpServer`): `GET /.well-known/jwks.json`; `POST /tokens` nhận body JSON `{tenant, principal, device, topics?, ttlSec?}` (mặc định `ttlSec=600`) và trả `{"token": "..."}`; `start()`, `stop()`, `port()`.
  - CLI (picocli, `Main` dùng `CommandLine` với các subcommand):
    - `issuer --port 8090 --key <pem>`: tự tạo key nếu chưa có.
    - `token --key <pem> --tenant --principal --device [--topic]* [--ttl 10m]`: in JWT ra stdout.
    - `health <url>`: exit 0 nếu HTTP 200, ngược lại 1; timeout 2 s.
- `pom.xml`: dependency `platform-contracts`, `nimbus-jose-jwt`, `bcpkix-jdk18on`, `picocli`, `netty-all` hoặc các module `netty-codec-http` + `netty-handler` + `netty-tcnative-boringssl-static` (dùng ở Task 5). `spring-boot-maven-plugin` goal `repackage` với `<classifier>exec</classifier>` và `<mainClass>com.pangate.devtools.Main</mainClass>`, để jar gốc vẫn dùng được như thư viện.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void createsKeyOnceAndReloadsSameKid(@TempDir Path d) {
  ECKey a = DevKeys.loadOrCreateEs256(d.resolve("es256.pem"));
  ECKey b = DevKeys.loadOrCreateEs256(d.resolve("es256.pem"));
  assertThat(b.getKeyID()).isEqualTo(a.getKeyID()).isEqualTo(a.computeThumbprint().toString());
  assertThat(Files.readString(d.resolve("es256.pem"))).startsWith("-----BEGIN EC PRIVATE KEY-----");
}
@Test void mintedTokenHasRequiredClaims() {
  // verify bằng ECDSAVerifier; aud=[pangate-gw], sub=t1:u1, tenant=t1, device=d1, jti khác rỗng,
  // exp - nbf = 600 s, header kid = key.getKeyID()
}
@Test void issuerServesJwksAndMintsTokens() {
  // GET /.well-known/jwks.json → 200, có 1 key với kid đúng, không có "d"
  // POST /tokens {"tenant":"t1","principal":"t1:u1","device":"d1"} → JWT verify được
}
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-devtools test`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực các class và lệnh**

- [ ] **Step 4: Chạy để thấy pass, kiểm tra CLI**

Run: `./mvnw -f gateway-devtools install && java -jar gateway-devtools/target/gateway-devtools-0.1.0-SNAPSHOT-exec.jar token --key /tmp/k.pem --tenant t1 --principal t1:u1 --device d1`
Expected: test pass; lệnh in ra một JWT có 3 phần ngăn cách bằng dấu chấm.

- [ ] **Step 5: Commit**

```bash
git add gateway-devtools
git commit -m "feat(devtools): dev ES256 keys, JWT minter, issuer server and token/health commands"
```

---

### Task 5: gateway-devtools — `GatewayClient`, `client`, `smoke`

**Files:**
- Create: `src/main/java/com/pangate/devtools/client/{GatewayClient,ClientTls,Hellos,CloseInfo,GatewayClosedException}.java`, `cmd/{ClientCommand,SmokeCommand}.java`
- Test: `src/test/java/com/pangate/devtools/client/GatewayClientTest.java`, `client/FakeGatewayServer.java` (test helper)

**Interfaces:**
- Consumes: `Frame`, `Hello`, `Connected`, `ErrorInfo` (Task 2); `JwtMinter`, `DevKeys` (Task 4).
- Produces:
  - `ClientTls.trust(Path caCert)`, `ClientTls.insecure()`.
  - `GatewayClient.connect(URI url, ClientTls tls, Duration timeout)` và overload có thêm `InetSocketAddress localBind`. URL `wss://host:port/v1/ws` (WS, subprotocol `pangate.v1`) hoặc `tcps://host:port` (TCP + TLS, ALPN `pangate.v1`, length prefix 4 byte).
  - Instance methods:
    - `void send(Frame f)`
    - `Connected hello(Hello h, Duration timeout)`: ném `GatewayClosedException` (có `closeInfo()`) nếu bị đóng.
    - `Frame await(FrameType type, Duration timeout)`
    - `Frame ping(Duration timeout)`
    - `CloseInfo awaitClose(Duration timeout)` với `record CloseInfo(int code, String reason)`. Với TCP, code lấy từ `ErrorInfo.code` khi `fatal=true`.
    - `boolean isOpen()`, `void close()` (WS: close frame 1000), `void abort()` (đóng socket, không gửi close frame).
  - `Hellos.jwt(String jwt, String deviceId)`, `Hellos.ticket(String ticket, String deviceId)`: `sdk_version="devtools-0.1.0"`, `platform="iot"`.
  - Tất cả client dùng chung một NIO event loop group daemon (`gw-client-*`).
  - CLI:
    - `client --url <u> (--ca <crt>|--insecure) (--token <jwt>|--ticket-file <f>) --device <d> [--once] [--load N --ramp R] [--bind 127.0.0.1,127.0.0.2] [--ping-interval 30s]`. `--once`: HELLO → PING → close, exit 0. `--load`: mở N kết nối với tốc độ R/s, gán địa chỉ `--bind` xoay vòng, ping định kỳ, mỗi 10 s in `connected=… failed=…`. Lưu ticket mới nhất vào `--ticket-file` nếu có.
    - `smoke --url <u> --issuer <http> (--ca|--insecure)`: lấy token qua `POST /tokens` → HELLO → `CONNECTED` → PING/PONG → close → HELLO lại bằng ticket → `CONNECTED` với `session_expires_at_ms` không đổi → close. Exit 0 nếu mọi bước đúng; ngược lại in bước lỗi và exit 1.

- [ ] **Step 1: Viết test thất bại**

`FakeGatewayServer` là server Netty WSS tối giản trong test, dùng `SelfSignedCertificate` của `netty-pkitesting`: nhận `HELLO` → trả `CONNECTED` (`conn_id="01TEST"`); `PING` → `PONG`; `HELLO` có `device_id="kick"` → close 4012 với reason `"DUPLICATE_SESSION"`.

```java
@Test void helloReturnsConnected() { /* hello(...).getConnId() == "01TEST" */ }
@Test void pingReturnsPong() { /* ping(...).getType() == FRAME_TYPE_PONG */ }
@Test void serverCloseSurfacesCode() {
  assertThatThrownBy(() -> client.hello(Hellos.jwt("x", "kick"), Duration.ofSeconds(2)))
      .isInstanceOfSatisfying(GatewayClosedException.class, e -> assertThat(e.closeInfo().code()).isEqualTo(4012));
}
@Test void tcpTransportWorks() { /* FakeGatewayServer cũng mở listener tcps với ALPN pangate.v1 */ }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-devtools test -Dtest=GatewayClientTest`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực `GatewayClient`, `ClientTls`, `Hellos`, các lệnh `client` và `smoke`**

Thêm test dependency `io.netty:netty-pkitesting` (version theo netty-bom) cho `FakeGatewayServer`.

Pipeline WS: `SslHandler → HttpClientCodec → HttpObjectAggregator(8192) → WebSocketClientProtocolHandler(subprotocol pangate.v1, handleCloseFrames=false) → handler nhận frame`. Pipeline TCP: `SslHandler(ALPN pangate.v1) → LengthFieldBasedFrameDecoder(66560,0,4,0,4) + LengthFieldPrepender(4) → handler`. Frame nhận được đi vào một `LinkedBlockingQueue<Frame>`.

- [ ] **Step 4: Chạy để thấy pass**

Run: `./mvnw -f gateway-devtools install`
Expected: mọi test pass.

- [ ] **Step 5: Commit**

```bash
git add gateway-devtools
git commit -m "feat(devtools): gateway client (WSS/TCP) with client and smoke commands"
```

---

### Task 6: gateway-service skeleton — properties, limits, profiles

**Files:**
- Create: `gateway-service/pom.xml`; `src/main/java/com/pangate/gateway/GatewayApplication.java`; `config/{GatewayProperties,GatewayLimits,LimitsHolder}.java`; `server/{NodeState,TickScheduler,Transport}.java`; `src/main/resources/{application.yml,application-local.yml,application-dev.yml}`
- Test: `src/test/java/com/pangate/gateway/config/GatewayPropertiesTest.java`; `src/test/java/com/pangate/gateway/test/BlockHoundExtension.java`; `src/test/resources/{junit-platform.properties,META-INF/services/org.junit.jupiter.api.extension.Extension}`
- Modify: `Makefile` (thêm `gateway-service` vào `MODULES` nếu Task 1 chưa có)

**Interfaces:**
- Produces:
  - `@ConfigurationProperties("gateway") record GatewayProperties(String nodeId, String cell, Listeners listeners, Io io, Tls tls, Limits limits, Admission admission, Auth auth, Dev dev, Startup startup)` với các record lồng nhau:
    - `Listeners(Ws ws, Tcp tcp)`, `Ws(int port, String path, int acceptors)`, `Tcp(int port, boolean enabled)`
    - `Io(int threads, Transport transport)`, với `enum Transport { AUTO, EPOLL, NIO }` tạo ở file `server/Transport.java`
    - `Tls(Path certFile, Path keyFile)`
    - `Limits(int hardCap, int softCap, int perIp, int frameMax, int helloMax, Duration helloTimeout, Duration handshakeTimeout, int handshakesPerPrincipalPerMin, Duration heartbeatInterval, Duration sessionTtl)`
    - `Admission(int rate, int burst, int rampFrom, Duration rampDuration)`
    - `Auth(String jwksUri, Path jwksFile, Duration jwksRefresh, String audience, String issuer, Duration clockSkew)`
    - `Dev(boolean bootstrapSecrets, Path dir)`, `Startup(Duration dependencyTimeout)`
  - `record GatewayLimits(...)` gồm đúng các field của `Limits`, cộng `static GatewayLimits from(GatewayProperties p)` và `long idleTimeoutSec()` (= 3 × heartbeat).
  - `final class LimitsHolder { GatewayLimits current(); void set(GatewayLimits l); }` (dùng `AtomicReference`).
  - `final class NodeState { volatile boolean draining; volatile boolean listenersBound; }` với getter và setter.
  - `TickScheduler`: bean bọc `ScheduledExecutorService` một thread tên `gw-tick`, có `ScheduledExecutorService executor()`, `scheduleAtFixedRate(Runnable, Duration period)` và `shutdown()`.
  - `GatewayApplication`: `@SpringBootApplication @ConfigurationPropertiesScan`.
- Giá trị trong `application.yml` (prod mặc định) lấy đúng từ Global Constraints. Thêm `gateway.auth.audience: pangate-gw`, `gateway.auth.clock-skew: 30s`, `gateway.auth.jwks-refresh: 5m`, `gateway.io.threads: 0`, `gateway.io.transport: auto`, `gateway.listeners.ws.acceptors: 4`, cùng khối `server:` và `management:` copy nguyên từ spec §9 (port 8081, giới hạn thread Tomcat, health group `readiness`/`liveness` với `additional-path` `server:/ready` và `server:/live`, expose `health,prometheus,loggers`).
- `application-local.yml`:
  - `gateway.node-id=gateway-local`, `gateway.cell=cell-1`
  - `gateway.dev.bootstrap-secrets=true`, `gateway.dev.dir=${user.home}/.pangate-dev`
  - `gateway.tls.cert-file=${gateway.dev.dir}/gateway.crt`, `gateway.tls.key-file=${gateway.dev.dir}/gateway.key`
  - `gateway.auth.jwks-file=${gateway.dev.dir}/jwks.json`, `gateway.auth.jwks-uri=""`
  - `pangate.secrets.dir=${gateway.dev.dir}`
  - Giới hạn local theo Global Constraints.
- `application-dev.yml`: `gateway.dev.dir=/dev-secrets`, `gateway.auth.jwks-uri=http://issuer:8090/.well-known/jwks.json`, `gateway.auth.jwks-file=""`, hard-cap 200000, `gateway.node-id=gateway-dev-1`.
- `pom.xml`:
  - Dependency: `platform-contracts`, `platform-spring-starter`, `spring-boot-starter-web`, `spring-boot-starter-actuator`, `micrometer-registry-prometheus`, `netty-codec-http`, `netty-handler`, `netty-transport-classes-epoll`, `netty-transport-native-epoll` (classifier `linux-x86_64` và `linux-aarch_64`), `netty-tcnative-boringssl-static` (kèm classifier tương ứng), `fastutil`, `nimbus-jose-jwt`, `bcpkix-jdk18on`.
  - Test: `spring-boot-starter-test`, `awaitility`, `blockhound`, `gateway-devtools` (scope `test`), `netty-pkitesting`.
  - Enforcer `bannedDependencies` theo Global Constraints.
  - `build-helper-maven-plugin` thêm `src/it/java` làm test source; surefire và failsafe (include `**/*IT.java`) với `argLine=-XX:+AllowRedefinitionToAddDeleteMethods`; `spring-boot-maven-plugin` `repackage`.
- `BlockHoundExtension` (autodetect qua `junit-platform.properties`: `junit.jupiter.extensions.autodetection.enabled=true`): gọi `BlockHound.install` **một lần** với `nonBlockingThreadPredicate` đánh dấu thread có tên bắt đầu bằng `gw-io-` là non-blocking.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void prodDefaultsMatchSpec() {
  // server.port == 8081; management.endpoint.health.group.readiness.additional-path == "server:/ready"
  // ApplicationContextRunner + ConfigDataApplicationContextInitializer, không có profile
  // limits: hardCap 700000, softCap 550000, perIp 200, frameMax 65536, helloMax 8192,
  // helloTimeout 5s, handshakeTimeout 10s, heartbeat 30s, sessionTtl 24h, handshakesPerPrincipalPerMin 20;
  // admission 5000/10000/500/2m; GatewayLimits.from(p).idleTimeoutSec() == 90
}
@Test void localProfileOverrides() { /* profile local: hardCap 100000, perIp 100000, devDir = user.home/.pangate-dev, jwksFile set */ }
@Test void devProfileUsesIssuerJwks() { /* profile dev: jwksUri = http://issuer:8090/.well-known/jwks.json, hardCap 200000 */ }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-service test -Dtest=GatewayPropertiesTest`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực các class, file yml và `pom.xml`**

- [ ] **Step 4: Chạy để thấy pass, kiểm tra enforcer**

Run: `./mvnw -f gateway-service verify -DskipITs`
Expected: 3 test pass; enforcer pass.

- [ ] **Step 5: Commit**

```bash
git add gateway-service Makefile
git commit -m "feat(gateway): service skeleton with typed properties and local/dev profiles"
```

---

### Task 7: Dev secrets bootstrap và startup diagnostics

**Files:**
- Create: `dev/{DevSecretsEnvironmentPostProcessor,DevSecretsGenerator}.java`; `diagnostics/{ListenerBindException,ListenerBindFailureAnalyzer,SecretNotFoundFailureAnalyzer,JwksConfigFailureAnalyzer}.java`; `auth/JwksConfigException.java`; `src/main/resources/META-INF/spring.factories`
- Test: `src/test/java/com/pangate/gateway/dev/DevSecretsGeneratorTest.java`, `dev/DevSecretsEnvironmentPostProcessorTest.java`, `diagnostics/FailureAnalyzersTest.java`

**Interfaces:**
- Produces:
  - `DevSecretsGenerator.ensure(Path dir)`: chỉ sinh **những file còn thiếu** theo bố cục ở Global Constraints. Mỗi file được ghi ra file tạm rồi `ATOMIC_MOVE`. Thư mục `0700`, file key `0600` (nếu filesystem hỗ trợ POSIX).
    - CA: EC P-256, tự ký, sống 10 năm.
    - `gateway.crt`: ký bởi CA, 825 ngày, SAN `DNS:localhost`, `DNS:gateway`, `IP:127.0.0.1`, `IP:::1`, EKU `serverAuth`.
    - `gateway.key`: PKCS#8 (`JcaPKCS8Generator`).
    - `es256.pem` + `jwks.json`: cùng định dạng và `kid` với `DevKeys` (Task 4), sinh bằng nimbus + BouncyCastle ngay trong gateway, không phụ thuộc devtools.
    - `ticket-keys/{prev,current,next}`: kid 1, 2, 3.
    - `principal-hash.key`.
  - `DevSecretsEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered` (đăng ký trong `spring.factories`, order `LOWEST_PRECEDENCE`):
    - `gateway.dev.bootstrap-secrets=true` và profile `prod` đang bật → ném `IllegalStateException("gateway.dev.bootstrap-secrets must not be enabled with profile prod")`.
    - `true` và đang ở profile `local` hoặc `dev` → `ensure(gateway.dev.dir)`.
    - Các trường hợp còn lại: không làm gì.
  - `ListenerBindException(String listener, int port, String envVar, Throwable cause)`. Các `FailureAnalyzer` (đăng ký trong `spring.factories`) in ra **một dòng** mô tả kèm **một dòng** hành động:
    - Bind lỗi: `Port 8443 (ws) is already in use` / `Stop the other process or set GATEWAY_LISTENERS_WS_PORT`.
    - `SecretNotFoundException`: `Secret 'gateway.key' not found at <path>` / `Run with profile local (auto-generates dev secrets) or provide the file`.
    - `JwksConfigException`: thông điệp của exception / `Set exactly one of gateway.auth.jwks-uri or gateway.auth.jwks-file`.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void generatesAllFilesWithExpectedFormats(@TempDir Path d) {
  DevSecretsGenerator.ensure(d);
  // tồn tại đủ 10 file; gateway.key bắt đầu "-----BEGIN PRIVATE KEY-----"; es256.pem bắt đầu "-----BEGIN EC PRIVATE KEY-----";
  // cert có SAN chứa "localhost" và "127.0.0.1", key EC P-256, chuỗi ký verify được bằng public key của ca.crt;
  // jwks.json có đúng 1 key, kid == thumbprint của es256.pem;
}
@Test void secondRunKeepsExistingFiles(@TempDir Path d) { /* ensure 2 lần → bytes của mọi file không đổi */ }
@Test void fillsOnlyMissingFiles(@TempDir Path d) { /* xoá jwks.json → ensure → jwks.json được tạo lại, kid không đổi */ }
@Test void prodProfileWithBootstrapFails() { /* postProcessEnvironment với profile prod + flag true → IllegalStateException */ }
@Test void noProfileDoesNothing(@TempDir Path d) { }
@Test void bindFailureAnalysisNamesPortAndEnvVar() { /* analyze(ListenerBindException("ws",8443,"GATEWAY_LISTENERS_WS_PORT",…)) */ }
@Test void missingSecretAnalysisNamesPath() { }
```

Ticket key: `prev=1`, `current=2`, `next=3`. Test kiểm `ticket-keys/current` khớp `^2 [A-Za-z0-9+/]{43}=$`.

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-service test -Dtest='DevSecrets*Test,FailureAnalyzersTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực các class và `spring.factories`**

- [ ] **Step 4: Chạy để thấy pass**

Run: `./mvnw -f gateway-service test -Dtest='DevSecrets*Test,FailureAnalyzersTest'`
Expected: mọi test pass.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): auto-generate dev secrets and readable startup failure analysis"
```

---

### Task 8: `ConnId` và `DeadlineWheel`

**Files:**
- Create: `session/ConnId.java`, `loop/{WheelNode,DeadlineWheel}.java`
- Test: `src/test/java/com/pangate/gateway/session/ConnIdTest.java`, `loop/DeadlineWheelTest.java`

**Interfaces:**
- Produces:
  - `ConnId.next(long nowMillis, int loopIdx, long sessionKey, RandomGenerator rnd) -> String`: bố cục bit `ts48 | loopIdx8 | sessionKey48 | rand24`, mã Crockford base32 thành 26 ký tự (kiểu ULID). `ConnId.loopIdx(String) -> int`, `ConnId.sessionKey(String) -> long`, `ConnId.isValid(String) -> boolean`.
  - `abstract class WheelNode` với các field package-private `long deadlineSec; WheelNode prev, next; int slot = -1` và `boolean isScheduled()`.
  - `DeadlineWheel(int slots, long startSec)`:
    - `void schedule(WheelNode n, long deadlineSec)`: gỡ node khỏi slot cũ nếu có; deadline ≤ giây đã tick cuối → đặt vào slot của giây kế tiếp.
    - `void remove(WheelNode n)`: O(1), gọi nhiều lần vẫn an toàn.
    - `void advanceTo(long nowSec, Consumer<WheelNode> onDue)`: xử lý lần lượt từng giây từ `lastTick+1` tới `nowSec`. Ở mỗi slot, node đến hạn bị gỡ ra rồi mới gọi callback (callback được phép `schedule` lại); node chưa đến hạn để nguyên tại chỗ (lazy).
    - `int size()`.

- [ ] **Step 1: Viết test thất bại**

```java
// ConnIdTest
@Test void encodesTwentySixCrockfordChars() { /* matches ^[0-9A-HJKMNP-TV-Z]{26}$ */ }
@Test void roundTripsLoopAndSessionKey() {
  long key = (5L << 40) | 123_456L;
  String id = ConnId.next(1_762_000_000_000L, 5, key, new SplittableRandom(1));
  assertThat(ConnId.loopIdx(id)).isEqualTo(5); assertThat(ConnId.sessionKey(id)).isEqualTo(key);
}
@Test void sortsByTimestamp() { /* id(ts=1000) < id(ts=2000) theo String.compareTo */ }
@Test void uniqueAcross100kIds() { }
@Test void rejectsInvalid() { /* isValid("short")==false; loopIdx("I…") → IllegalArgumentException */ }
// DeadlineWheelTest (slots=128, start=0)
@Test void firesAtDeadlineNotBefore() { /* schedule(5); advanceTo(4) none; advanceTo(5) once */ }
@Test void farDeadlineFiresOnlyWhenDue() { /* schedule(300); advanceTo(299) none; advanceTo(300) once */ }
@Test void catchesUpAfterStall() { /* schedule 3 và 7; advanceTo(10) → [3,7] theo thứ tự */ }
@Test void removeIsIdempotent() { }
@Test void callbackMayReschedule() { /* fire ở 5, reschedule 10 → fire ở 10; size()==1 giữa hai lần */ }
@Test void pastDeadlineFiresOnNextTick() { /* advanceTo(20); schedule(10) → advanceTo(21) fires */ }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-service test -Dtest='ConnIdTest,DeadlineWheelTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực**

Slot của một deadline = `deadlineSec % slots`. Mỗi slot là danh sách liên kết đôi intrusive (con trỏ `head` của slot nằm trong mảng `WheelNode[] heads`).

- [ ] **Step 4: Chạy để thấy pass**

Run: `./mvnw -f gateway-service test -Dtest='ConnIdTest,DeadlineWheelTest'`
Expected: 11 test pass.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): self-routing connId and per-loop deadline wheel"
```

---

### Task 9: `IoGroups`, `LoopContext`, `LoopRegistry`

**Files:**
- Create: `server/IoGroups.java`, `loop/{LoopContext,LoopRegistry}.java`
- Test: `src/test/java/com/pangate/gateway/server/IoGroupsTest.java`, `loop/LoopContextTest.java`

**Interfaces:**
- Consumes: `DeadlineWheel`, `WheelNode` (Task 8); `GatewayProperties.Io` (Task 6).
- Produces:
  - `IoGroups(GatewayProperties.Io io)`:
    - `Transport transport()`: `AUTO` → `EPOLL` nếu `Epoll.isAvailable()`, ngược lại `NIO`.
    - `EventLoopGroup group()`: `MultiThreadIoEventLoopGroup(threads, EpollIoHandler.newFactory() | NioIoHandler.newFactory())`, thread factory `DefaultThreadFactory("gw-io")`; `threads=0` nghĩa là `Runtime.availableProcessors()`.
    - `Class<? extends ServerChannel> serverChannelClass()`.
    - `int effectiveAcceptors(int configured)`: ép về 1 và log INFO khi không phải epoll.
    - `Future<?> shutdown(Duration quiet, Duration timeout)`.
  - `LoopContext(int index, EventLoop loop, int wheelSlots, LongSupplier epochSec)`:
    - `index()`, `eventLoop()`, `inLoop()`, `nowSec()`
    - `long nextSessionKey()` = `((long) index << 40) | counter++`
    - `void register(Session s)`, `void unregister(Session s)`, `Session session(long key)`, `void forEachSession(Consumer<Session>)`
    - `int activeCount()` (volatile, đếm channel đã qua admission, kể cả đang handshake), `void incrementActive()`, `void decrementActive()`
    - `DeadlineWheel wheel()`
    - `void startTicker()`: `eventLoop.scheduleAtFixedRate(…, 1, 1, SECONDS)` gọi `wheel.advanceTo(nowSec(), n -> ((Session) n).onDeadline(nowSec()))`.
    - Kiểu `Session` do Task 14 định nghĩa. Ở task này dùng `WheelNode` và `Session` như một forward reference: tạo `Session` rỗng `public final class Session extends WheelNode { void onDeadline(long now) {} }` để compile, Task 14 sẽ thay bằng bản đầy đủ.
  - `LoopRegistry.create(EventLoopGroup group, int wheelSlots, LongSupplier epochSec)`: duyệt `group` theo thứ tự iterator, gán index từ 0. Các method: `LoopContext get(int idx)`, `LoopContext of(EventLoop loop)` (IdentityHashMap), `int size()`, `List<LoopContext> all()`, `int totalActive()`.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void autoPicksEpollOnLinux() { assumeTrue(Epoll.isAvailable()); /* transport()==EPOLL, effectiveAcceptors(4)==4 */ }
@Test void nioClampsAcceptorsToOne() { /* Io(2, NIO) → effectiveAcceptors(4)==1; thread name bắt đầu "gw-io-" */ }
@Test void registryIndexesEveryLoop() { /* group 4 thread → size 4, of(get(i).eventLoop()).index()==i */ }
@Test void sessionKeysEmbedLoopIndex() { /* LoopContext index 3 → nextSessionKey() >>> 40 == 3, tăng dần */ }
@Test void tickerAdvancesWheelOnLoop() { /* clock giả, schedule node deadline now+1, chờ ≤ 3 s thấy onDeadline chạy trên thread gw-io-* */ }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-service test -Dtest='IoGroupsTest,LoopContextTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực**

- [ ] **Step 4: Chạy để thấy pass**

Run: `./mvnw -f gateway-service test -Dtest='IoGroupsTest,LoopContextTest'`
Expected: mọi test pass (test epoll được bỏ qua nếu môi trường không có epoll).

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): io groups with epoll/nio selection and per-loop context registry"
```

---

### Task 10: Protocol — `CloseCode`, `ChannelCloser`, `FrameCodec`, `WsBinaryAdapter`, `ErrorTerminator`

**Files:**
- Create: `protocol/{CloseCode,ProtocolException,ChannelAttrs,ChannelCloser,Frames,FrameCodec}.java`, `pipeline/{WsBinaryAdapter,ErrorTerminator}.java`
- Test: `src/test/java/com/pangate/gateway/protocol/{FrameCodecTest,ChannelCloserTest}.java`, `pipeline/WsBinaryAdapterTest.java`

**Interfaces:**
- Consumes: `Frame`, `ErrorInfo` (Task 2); `LimitsHolder` (Task 6).
- Produces:
  - `enum CloseCode { NORMAL(1000), GOING_AWAY(1001), ABNORMAL(1006), BAD_FRAME(4000), AUTH_FAILED(4001), SESSION_EXPIRED(4002), REVOKED(4003), HELLO_TIMEOUT(4004), SLOW_CONSUMER(4008), RATE_LIMITED(4009), UPGRADE_REQUIRED(4010), WRONG_CELL(4011), DUPLICATE_SESSION(4012), OVERLOADED(4013), INTERNAL(4500) }`, mỗi giá trị có `int code()`, `boolean retryable()`, `long defaultRetryAfterMs()` (`RATE_LIMITED=30000`, `OVERLOADED=15000`, còn lại 0). `ABNORMAL` chỉ dùng nội bộ, không bao giờ gửi đi.
  - `ProtocolException(CloseCode code, String reason)`.
  - `ChannelAttrs`:
    - `TRANSPORT`: `AttributeKey<TransportKind>`, `enum TransportKind { WS, TCP }`.
    - `PROTOCOL_READY`: `AttributeKey<Boolean>`; WS bật sau khi upgrade xong, TCP bật sau TLS + ALPN.
    - `AUTHENTICATED`: `AttributeKey<Boolean>`.
    - `CLOSING`: `AttributeKey<Boolean>`, để `ChannelCloser` chỉ đóng một lần.
    - `CLIENT_CLOSE_CODE`: `AttributeKey<Integer>`, code trong close frame client gửi.
  - `ChannelCloser.close(Channel ch, CloseCode code, String reason, long retryAfterMs) -> ChannelFuture`:
    - Chưa `PROTOCOL_READY` → `ch.close()`.
    - WS → ghi `CloseWebSocketFrame(code, reason + (retryAfterMs > 0 ? " retryAfter=" + retryAfterMs : ""))` rồi close.
    - TCP → ghi `Frame(ERROR, ErrorInfo{code, message=reason, retryable, retry_after_ms, fatal=true})` rồi close.
    - Mỗi channel chỉ đóng một lần (attr `CLOSING`).
  - `Frames`: các factory tĩnh `pong(String msgId)`, `connected(Connected c)`, `error(int code, String message, boolean retryable, boolean fatal)`, `reauth(Reauth r)`.
  - `FrameCodec extends MessageToMessageCodec<ByteBuf, Frame>` (mỗi channel một instance), constructor `FrameCodec(LimitsHolder limits)`:
    - Giới hạn kích thước: `helloMax` khi `AUTHENTICATED` chưa bật, `frameMax` khi đã bật; vượt → `ProtocolException(BAD_FRAME, "frame too large")`.
    - Parse lỗi → `BAD_FRAME`.
    - Chỉ nhận các loại frame theo chiều client→server: `HELLO, PING, ACK, UPSTREAM, SUBSCRIBE, UNSUBSCRIBE, RPC_RES, REAUTH`; loại khác → `BAD_FRAME`.
  - `WsBinaryAdapter`: `BinaryWebSocketFrame` → `ByteBuf`; `TextWebSocketFrame` → `BAD_FRAME`; `CloseWebSocketFrame` từ client → `ch.attr(ChannelAttrs.CLIENT_CLOSE_CODE).set(code)` rồi `ch.close()`. Chiều ra: bọc `ByteBuf` thành `BinaryWebSocketFrame`.
  - `ErrorTerminator` (`@Sharable`, handler cuối pipeline): `ProtocolException` → `ChannelCloser.close(code, reason)`; `IOException` (client reset) → `ch.close()`; exception khác → `INTERNAL` và log WARN mẫu.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void decodesValidFrame() { }
@Test void rejectsHelloLargerThan8192BeforeAuth() { /* 8193 byte → channel đóng; outbound không có frame (chưa PROTOCOL_READY) */ }
@Test void allows65536AfterAuthRejects65537() { }
@Test void rejectsGarbageAsBadFrame() { }
@Test void rejectsServerOnlyTypeFromClient() { /* FRAME_TYPE_CONNECTED từ client → BAD_FRAME */ }
@Test void wsCloseCarriesCodeAndRetryAfter() { /* PROTOCOL_READY, WS: close(RATE_LIMITED,"slow down",30000) → CloseWebSocketFrame 4009, reason chứa "retryAfter=30000" */ }
@Test void tcpCloseSendsFatalErrorFrame() { /* frame ERROR với ErrorInfo.code=4013, fatal=true, retry_after_ms=15000 */ }
@Test void closeTwiceWritesOnce() { }
@Test void textWebSocketFrameIsBadFrame() { }
@Test void clientCloseFrameRecordsCode() { /* CloseWebSocketFrame(1000) → attr CLIENT_CLOSE_CODE == 1000, channel đóng */ }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-service test -Dtest='FrameCodecTest,ChannelCloserTest,WsBinaryAdapterTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực**

- [ ] **Step 4: Chạy để thấy pass**

Run: `./mvnw -f gateway-service test -Dtest='FrameCodecTest,ChannelCloserTest,WsBinaryAdapterTest'`
Expected: mọi test pass.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): frame codec, close codes and transport-aware channel closing"
```

---

### Task 11: Resume ticket

**Files:**
- Create: `auth/{TicketKey,TicketKeyRing,ResumeTicket,ResumeTicketService,TicketVerifyResult}.java`
- Test: `src/test/java/com/pangate/gateway/auth/ResumeTicketServiceTest.java`

**Interfaces:**
- Consumes: `SecretSource` (Task 3).
- Produces:
  - `record TicketKey(int kid, byte[] key)`; `TicketKeyRing.load(SecretSource s)` đọc `ticket-keys/{prev,current,next}`, định dạng `"<kid> <base64>"`, key phải dài đúng 32 byte. Method `TicketKey current()`, `Optional<TicketKey> byKid(int kid)`.
  - `record ResumeTicket(String cell, String tenant, String principal, String device, long authAtSec, long expSec, long authJtiHash, List<String> topicPatterns)`. Constructor kiểm: tối đa 8 pattern, mỗi pattern ≤ 64 byte UTF-8; vi phạm → `IllegalArgumentException`.
  - `ResumeTicketService(TicketKeyRing ring)`: `String issue(ResumeTicket t)`, `TicketVerifyResult verify(String token, long nowSec)`.
  - `sealed interface TicketVerifyResult permits Valid, Expired, Invalid` với `record Valid(ResumeTicket t)`, `record Expired(ResumeTicket t)`, `record Invalid(String reason)`.
  - Định dạng nhị phân đúng spec §4.7 (ver=1; chuỗi = độ dài `uint16` + byte UTF-8), base64url không padding, tổng ≤ 2048 byte. So HMAC bằng `MessageDigest.isEqual`.
  - `static long jtiHash(String jti)`: 8 byte đầu của SHA-256(jti), big-endian.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void issueThenVerifyRoundTrips() { }
@Test void tamperedByteIsInvalid() { /* lật 1 byte trong payload → Invalid */ }
@Test void unknownKidIsInvalid() { }
@Test void verifiesTicketSignedWithPreviousKey() { /* ký bằng ring có current=prev cũ; verify bằng ring mới → Valid (Review Focus 4) */ }
@Test void expiredTicketReportsExpired() { /* now = exp → Expired */ }
@Test void rejectsMoreThanEightPatternsOrLongPattern() { }
@Test void tokenIsBase64UrlUnder2048() { /* 8 pattern × 64 byte → ≤ 2048, khớp ^[A-Za-z0-9_-]+$ */ }
@Test void truncatedTokenIsInvalidNotException() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-service test -Dtest=ResumeTicketServiceTest`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực**

- [ ] **Step 4: Chạy để thấy pass**

Run: `./mvnw -f gateway-service test -Dtest=ResumeTicketServiceTest`
Expected: 8 test pass.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): HMAC resume tickets with three live keys"
```

---

### Task 12: `JwksCache` và `JwtVerifier`

**Files:**
- Create: `auth/{JwksSource,HttpJwksSource,FileJwksSource,JwksSources,JwksCache,ConnectClaims,JwtResult,JwtVerifier}.java`
- Test: `src/test/java/com/pangate/gateway/auth/{JwksCacheTest,JwtVerifierTest}.java`

**Interfaces:**
- Consumes: `GatewayProperties.Auth`, `TickScheduler` (Task 6); `JwksConfigException` (Task 7).
- Produces:
  - `interface JwksSource { FetchResult fetch(String etag) throws IOException; }`
    - `sealed interface FetchResult permits Updated, NotModified`, với `record Updated(JWKSet set, String etag, Duration maxAge)` và `record NotModified(Duration maxAge)`. `maxAge` là `null` nếu không có header.
    - `HttpJwksSource(URI uri, HttpClient client, Duration timeout)`: gửi `If-None-Match`; 304 → `NotModified`; khác 200 → `IOException`.
    - `FileJwksSource(Path file)`: etag = `lastModified` tính bằng millis.
    - `JwksSources.from(GatewayProperties.Auth a, HttpClient c) -> JwksSource`: đặt cả hai hoặc không đặt cái nào → `JwksConfigException`.
  - `JwksCache(JwksSource src, ScheduledExecutorService tick, Executor offload, Duration refresh, LongSupplier nowMillis)`:
    - `void start()`: tải lần đầu ngay. Lỗi thì retry mỗi 10 s cho tới khi có tập key đầu tiên, sau đó refresh theo chu kỳ (kẹp `maxAge` vào [60 s, 3600 s]; file source cố định 30 s).
    - `void stop()`
    - `Optional<ECPublicKey> key(String kid)`
    - `CompletableFuture<Boolean> refreshFor(String kid)`: singleflight. Lần gọi trước còn chưa quá 10 s thì không fetch nữa, trả về ngay `key(kid).isPresent()`.
    - `int size()`, `long ageSeconds()` (−1 nếu chưa từng tải được)
    - `long refreshCount(String result)` với `result` ∈ `updated`, `not_modified`, `error`.
    - Chỉ giữ key `kty=EC`, `crv=P-256`, `use` rỗng hoặc `sig`, `alg` rỗng hoặc `ES256`. Tập key được thay toàn bộ qua `AtomicReference<Map<String, ECPublicKey>>`. Gặp lỗi không bao giờ xóa cache.
  - `record ConnectClaims(String tenant, String principal, String device, String jti, List<String> topics, long expSec)`.
  - `sealed interface JwtResult permits Verified, UnknownKid, Rejected`, với `record Verified(ConnectClaims c)`, `record UnknownKid(String kid)`, `record Rejected(String reason)`.
  - `JwtVerifier(JwksCache cache, String audience, String issuerOrEmpty, Duration clockSkew)`, method `JwtResult verify(String jwt, long nowSec)`:
    - Chỉ chấp nhận `alg=ES256`.
    - Claim bắt buộc: `aud`, `exp`, `nbf`, `sub`, `tenant`, `device`, `jti`.
    - `sub` phải bắt đầu bằng `tenant + ":"`.
    - `topics` là danh sách (có thể thiếu); tối đa 8 pattern, mỗi pattern ≤ 64 byte.
    - Cho phép lệch đồng hồ tối đa `clockSkew` với `exp` và `nbf`.

- [ ] **Step 1: Viết test thất bại**

```java
// JwksCacheTest — dùng executor đồng bộ (Runnable::run) và stub HttpServer có ETag
@Test void initialLoadPopulatesKeys() { }
@Test void refreshReplacesWholeSetAndDropsRemovedKid() { }
@Test void notModifiedKeepsKeys() { /* lần 2 server trả 304 → size giữ nguyên, refreshCount("not_modified")==1 */ }
@Test void errorKeepsLastGoodSet() { /* lần 2 server trả 500 → key(kid) vẫn có, refreshCount("error")==1 */ }
@Test void ignoresNonEs256Keys() { /* RSA và EC P-384 bị bỏ qua, size()==1 */ }
@Test void emergencyRefreshIsSingleflightAndRateLimited() { /* 2 lần refreshFor đồng thời → 1 hit; gọi lại sau 5 s → 0 hit */ }
@Test void maxAgeClampedToBounds() { /* max-age=5 → lần sau +60 s; max-age=99999 → +3600 s */ }
@Test void fileSourceDetectsRewrite(@TempDir Path d) { }
@Test void bothSourcesConfiguredIsConfigError() { }
// JwtVerifierTest
@Test void validTokenVerified() { /* claims khớp TokenRequest, dùng JwtMinter của devtools */ }
@Test void wrongAudienceRejected() { }
@Test void clockSkewWithin30sAccepted() { /* exp = now-20, nbf = now+20 → Verified (Review Focus 3) */ }
@Test void clockSkewBeyond30sRejected() { /* exp = now-31 → Rejected; nbf = now+31 → Rejected */ }
@Test void hs256AndNoneRejected() { }
@Test void unknownKidReported() { }
@Test void missingDeviceRejected() { }
@Test void subjectOutsideTenantRejected() { /* tenant=t1, sub=t2:u1 → Rejected */ }
@Test void tooManyTopicPatternsRejected() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-service test -Dtest='JwksCacheTest,JwtVerifierTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực**

`HttpJwksSource` chạy trên `offload` (`Executors.newVirtualThreadPerTaskExecutor()` do `GatewayConfig` cung cấp). `JwksCache` không bao giờ chạy trên event loop.

- [ ] **Step 4: Chạy để thấy pass**

Run: `./mvnw -f gateway-service test -Dtest='JwksCacheTest,JwtVerifierTest'`
Expected: mọi test pass.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): JWKS cache with periodic refresh and ES256 connect-token verifier"
```

---

### Task 13: Admission — `TokenBucket`, `AdmissionController`, `IpConnectionCounter`

**Files:**
- Create: `shed/{TokenBucket,AdmissionController,IpConnectionCounter}.java`
- Test: `src/test/java/com/pangate/gateway/shed/{TokenBucketTest,AdmissionControllerTest,IpConnectionCounterTest}.java`

**Interfaces:**
- Consumes: `GatewayProperties.Admission` (Task 6).
- Produces:
  - `TokenBucket(double ratePerSec, double burst, long nowNanos)`: không thread-safe, mỗi loop một bucket. Method `boolean tryAcquire(long nowNanos)`, `void setRate(double ratePerSec, double burst)`.
  - `AdmissionController(GatewayProperties.Admission a, int loops, LongSupplier nanoClock)`:
    - `void markReady()`: bắt đầu ramp; trước khi gọi, rate = `rampFrom`.
    - `double effectiveRate()`: tăng tuyến tính từ `rampFrom` lên `rate` trong `rampDuration`.
    - `boolean tryAdmit(int loopIdx)`: dùng bucket của loop với rate = `effectiveRate/loops`, burst = `max(1, burst/loops)`.
    - `void overrideBaseRate(int rate)`: dành cho kill switch ở M3; ở M1 chỉ dùng trong test.
  - `IpConnectionCounter`: `boolean tryIncrement(InetAddress a, int limit)`, `void decrement(InetAddress a)`, `int count(InetAddress a)`, `int trackedKeys()`, `static String key(InetAddress a)`. IPv4 dùng nguyên địa chỉ; IPv4-mapped IPv6 quy về IPv4; IPv6 lấy prefix /64. Bộ đếm về 0 thì xóa key.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void bucketRefillsAtRate() { /* rate 10, burst 2: 2 lần ok, lần 3 fail, +100 ms ok */ }
@Test void rampIsLinearFrom500To5000Over2Min() {
  // t=0 → 500; t=60 s → 2750; t≥120 s → 5000 (sai số 1e-6)
}
@Test void perLoopShareUsesRateDividedByLoops() { /* 4 loop, đã ramp xong: loop 0 nhận đúng burst 2500 lần liên tiếp */ }
@Test void ipv6SameSlash64SharesCounter() { /* 2001:db8::1 và 2001:db8::ffff cùng key; limit 1 → lần 2 fail (Review Focus 5) */ }
@Test void ipv4MappedIpv6CountsAsIpv4() { /* ::ffff:10.0.0.1 và 10.0.0.1 cùng key (Review Focus 5) */ }
@Test void decrementToZeroRemovesKey() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-service test -Dtest='TokenBucketTest,AdmissionControllerTest,IpConnectionCounterTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực**

- [ ] **Step 4: Chạy để thấy pass**

Run: `./mvnw -f gateway-service test -Dtest='TokenBucketTest,AdmissionControllerTest,IpConnectionCounterTest'`
Expected: mọi test pass.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): per-loop admission buckets with ramp and per-IP counters"
```

---

### Task 14: `Session`, `PrincipalIndex`, `SessionTracker`, `AcceptGate`

**Files:**
- Create: `session/{Session,SessionState,ConnRef,PrincipalIndex,SessionTracker}.java`, `pipeline/AcceptGate.java`, `metrics/GatewayMetrics.java`
- Modify: `session/Session.java`, thay bản stub của Task 9.
- Test: `src/test/java/com/pangate/gateway/session/{SessionTest,PrincipalIndexTest}.java`, `pipeline/AcceptGateTest.java`

**Interfaces:**
- Consumes: `LoopContext`, `LoopRegistry` (Task 9); `ChannelCloser`, `CloseCode` (Task 10); `AdmissionController`, `IpConnectionCounter` (Task 13); `NodeState`, `LimitsHolder` (Task 6); `ConnId` (Task 8).
- Produces:
  - `enum SessionState { HANDSHAKING, ACTIVE, CLOSED }`. `DRAINING` sẽ được thêm ở M3.
  - `record ConnRef(int loopIdx, long sessionKey)`.
  - `final class Session extends WheelNode`, giữ trong attr `Session.KEY`:
    - Đọc trạng thái: `loop()`, `channel()`, `sessionKey()`, `state()`, `connId()`, `identity()`, `authAtSec()`, `expAtSec()`, `authJtiHash()`, `topicPatterns()`, `lastSeenSec()`, `closeCode()`, `remoteAddress()`, `origin()`.
    - `void origin(String origin)`: `UpgradeGuard` ghi header `Origin` trước khi xác thực.
    - `void markUpgraded(long nowSec)`: đặt hạn HELLO = `now + helloTimeout`.
    - `void activate(Identity id, String connId, long authAtSec, long expAtSec, long authJtiHash, List<String> topics)`: chuyển sang `ACTIVE`, `lastSeen = now`.
    - `void reauth(long authAtSec, long expAtSec, long jtiHash, List<String> topics)`, `void touch(long nowSec)`.
    - `long nextDeadlineSec()`:
      - `HANDSHAKING`: `min(acceptedAt + handshakeTimeout, helloDeadline nếu đã upgrade)`.
      - `ACTIVE`: `min(lastSeen + idleTimeout, expAt)`.
    - `void onDeadline(long nowSec)`:
      - `HANDSHAKING` quá hạn → `close(HELLO_TIMEOUT)`.
      - `ACTIVE` mà `now ≥ expAt` → `close(SESSION_EXPIRED)`.
      - `ACTIVE` mà `now ≥ lastSeen + idleTimeout` → `close(GOING_AWAY, "HEARTBEAT_TIMEOUT")`.
      - Còn lại: `schedule` lại vào wheel theo `nextDeadlineSec()`.
    - `void close(CloseCode code, String reason)`: idempotent, ghi nhận `closeCode`, gọi `ChannelCloser`.
    - `record Identity(String tenantId, String principalId, String deviceId, String platform, String sdkVersion, Set<String> capabilities)`.
  - `PrincipalIndex`: `void add(String principal, ConnRef r)`, `void remove(String principal, ConnRef r)`, `List<ConnRef> get(String principal)`, `int principalCount()`. Bên trong là `ConcurrentHashMap<String, Object>`, value là `ConnRef` hoặc `ConnRef[]`, cập nhật bằng `compute`.
  - `SessionTracker(LoopRegistry, PrincipalIndex, IpConnectionCounter, LimitsHolder, GatewayMetrics, LongSupplier epochMillis)`:
    - `Session admit(Channel ch, LoopContext loop)`: tạo `Session(HANDSHAKING)`, gắn attr, `loop.register`, `incrementActive`, đặt deadline handshake vào wheel, gắn listener `closeFuture` gọi `onClosed`.
    - `void onActivated(Session s)`: thêm vào `PrincipalIndex`.
    - `void onClosed(Session s)` (chạy trên owner loop): gỡ khỏi wheel, `loop.unregister`, `decrementActive`, gỡ khỏi `PrincipalIndex`, `IpConnectionCounter.decrement`, ghi metric `closed(code)`. Nếu chưa có `closeCode` thì dùng `CLIENT_CLOSE_CODE` (1000 khi client gửi close frame), không có nữa thì `ABNORMAL`.
    - `String newConnId(Session s)`.
  - `AcceptGate` (`@Sharable`), xử lý `channelActive` theo thứ tự:
    1. `NodeState.draining` → `admissionRejected("draining")`, close.
    2. `loop.activeCount() ≥ ceil(hardCap / loops)` → `"hard_cap"`.
    3. `!ip.tryIncrement(addr, perIp)` → `"per_ip"`.
    4. `!admission.tryAdmit(loop.index())` → `"rate"`, đồng thời trả lại lượt đếm IP vừa tăng.

    Qua hết → `tracker.admit(...)`, `ctx.fireChannelActive()`. Bị từ chối thì đóng ngay, không bắt đầu TLS.
  - `GatewayMetrics(MeterRegistry r)`:
    - `handshake(String result)` → counter `gw.handshakes{result}`; `result` ∈ `ok, auth_failed, expired, wrong_cell, rate_limited, bad_frame, timeout`.
    - `admissionRejected(String reason)` → `gw.admission.rejected{reason}`.
    - `closed(CloseCode c)` → `gw.closes{code}`.
    - `bindLoops(LoopRegistry)` → gauge `gw.connections.active{loop}`.
    - `bindJwks(JwksCache)` → `gw.jwks.keys`, `gw.jwks.age` (giây), cùng FunctionCounter `gw.jwks.refresh{result}`.

- [ ] **Step 1: Viết test thất bại**

```java
// SessionTest (EmbeddedChannel + LoopContext trên EmbeddedEventLoop, clock giả)
@Test void handshakeTimesOutAfter10s() { /* không upgrade, advance 10 → channel đóng, closeCode HELLO_TIMEOUT */ }
@Test void helloDeadlineIs5sAfterUpgrade() { }
@Test void idleActiveSessionClosesAfter90s() { /* activate, advance 89 vẫn mở, 90 → đóng; reason HEARTBEAT_TIMEOUT, code 1001 */ }
@Test void touchPostponesIdleTimeout() { }
@Test void expiryClosesWith4002() { }
@Test void closeIsIdempotent() { }
// PrincipalIndexTest
@Test void multiDeviceAddRemove() { /* 2 ConnRef cho cùng principal → get trả cả 2; remove từng cái → principalCount()==0 */ }
// AcceptGateTest
@Test void rejectsWhenDraining() { }
@Test void rejectsAboveHardCapPerLoop() { }
@Test void rejectsPerIpAndRateWithReasons() { /* counter gw.admission.rejected{reason=per_ip|rate} tăng; đếm IP không bị rò khi rate fail */ }
@Test void admittedChannelTrackedAndCleanedOnClose() { /* activeCount 1 → close → 0; ip.trackedKeys()==0 */ }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-service test -Dtest='SessionTest,PrincipalIndexTest,AcceptGateTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực**

- [ ] **Step 4: Chạy để thấy pass**

Run: `./mvnw -f gateway-service test`
Expected: mọi test của gateway-service pass (gồm cả test các task trước).

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): session lifecycle, principal index, tracker and accept gate"
```

---

### Task 15: `SessionHandler` và `JwtAuthFlow`

**Files:**
- Create: `pipeline/SessionHandler.java`, `auth/JwtAuthFlow.java`
- Test: `src/test/java/com/pangate/gateway/pipeline/SessionHandlerTest.java`

**Interfaces:**
- Consumes: `Session`, `ResumeTicketService`, `JwtVerifier`, `JwksCache`, `Frames`, `ProtocolException`.
- Produces:
  - `JwtAuthFlow(JwtVerifier v, JwksCache c, Duration refreshWait /*2 s*/)`, method `void verify(Channel ch, String jwt, long nowSec, Consumer<JwtResult> onLoop)`. Kết quả `Verified` hoặc `Rejected` gọi `onLoop` ngay. `UnknownKid` thì `setAutoRead(false)`, `refreshFor(kid).orTimeout(refreshWait)`, sau đó `ch.eventLoop().execute(...)`: verify lại, `setAutoRead(true)`, rồi gọi `onLoop` với kết quả cuối (nếu vẫn `UnknownKid` thì quy về `Rejected("unknown kid")`).
  - `SessionHandler` (mỗi channel một instance). Mỗi frame vào đều gọi `session.touch(now)` trước khi xử lý. Xử lý theo loại frame:

  | Frame | Xử lý |
  |---|---|
  | `PING` | Trả `PONG`, giữ nguyên `msg_id` |
  | `HELLO` | `ProtocolException(BAD_FRAME, "duplicate HELLO")` |
  | `REAUTH{jwt}` | Verify qua `JwtAuthFlow`. `sub` và `device` phải khớp session, không khớp → `AUTH_FAILED`. Thành công → `session.reauth(now, now + sessionTtl, …)`, trả frame `REAUTH{resume_ticket, session_expires_at_ms}` |
  | `ACK` | Bỏ qua |
  | `UPSTREAM`, `SUBSCRIBE`, `UNSUBSCRIBE`, `RPC_RES` | Trả `ERROR{code=4500, message="UNSUPPORTED", retryable=false, fatal=false}`, giữ kết nối |

- [ ] **Step 1: Viết test thất bại**

```java
@Test void pingEchoesMsgIdAndTouches() { }
@Test void secondHelloIsBadFrame() { }
@Test void reauthExtendsExpiryAndReturnsNewTicket() { /* ticket mới verify ra authAt = now */ }
@Test void reauthForOtherPrincipalAuthFailed() { }
@Test void unsupportedFramesGetNonFatalError() { /* UPSTREAM → ERROR 4500 fatal=false; channel vẫn mở */ }
@Test void ackIsIgnored() { }
@Test void jwtAuthFlowRefreshesUnknownKidOnLoop() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-service test -Dtest=SessionHandlerTest`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực**

Test tạo session đã `ACTIVE` bằng `SessionTracker.admit` + `Session.activate`, không cần đi qua HELLO. Thêm test `jwtAuthFlowRefreshesUnknownKidOnLoop` cho `JwtAuthFlow`: `UnknownKid` → `autoRead` tắt trong lúc chờ → callback chạy trên event loop của channel → `autoRead` bật lại.

- [ ] **Step 4: Chạy để thấy pass**

Run: `./mvnw -f gateway-service test`
Expected: toàn bộ test pass.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): session handler with ping, reauth and unsupported-frame errors"
```

---

### Task 16: `HelloAuthHandler`

**Files:**
- Create: `pipeline/HelloAuthHandler.java`, `auth/PrincipalHandshakeLimiter.java`
- Test: `src/test/java/com/pangate/gateway/pipeline/HelloAuthHandlerTest.java`, `auth/PrincipalHandshakeLimiterTest.java`

**Interfaces:**
- Consumes: `JwtAuthFlow`, `SessionHandler` (Task 15); `ResumeTicketService` (Task 11); `SessionTracker`, `Session` (Task 14); `Frames`, `ChannelCloser`, `ChannelAttrs`, `ProtocolException` (Task 10); `GatewayProperties` (`cell`), `LimitsHolder`.
- Produces:
  - `PrincipalHandshakeLimiter(int perMinute, LongSupplier nowMillis)`: `boolean tryAcquire(String principal)` dùng cửa sổ trượt 60 s, thread-safe, tự dọn principal không hoạt động quá 120 s (khi có lời gọi).
  - `HelloAuthHandler` (mỗi channel một instance):
    - `userEventTriggered`: nhận `WebSocketServerProtocolHandler.HandshakeComplete` (WS) hoặc `SslHandshakeCompletionEvent` thành công (TCP, chỉ khi ALPN = `pangate.v1`, sai thì `ch.close()`) → bật `PROTOCOL_READY`, gọi `session.markUpgraded(now)`.
    - `channelRead(Frame)`: chỉ chấp nhận `HELLO`, frame khác → `ProtocolException(BAD_FRAME)`.
    - Khi xác thực thành công:
      - Bật attr `AUTHENTICATED`, gọi `session.activate(...)` và `tracker.onActivated`.
      - Gửi `CONNECTED` với:
        - `conn_id`, `cell = gateway.cell`
        - `heartbeat_sec = heartbeatInterval` (30)
        - `resume_ticket`, `server_time_ms`
        - `session_expires_at_ms = expAt × 1000`
        - `seq = 0`, `seq_epoch = 0`, `seq_unknown = true`
      - Tự gỡ khỏi pipeline, ghi metric `handshake("ok")`.
    - Thứ tự kiểm tra là a, b, c, h theo spec §4.6, ánh xạ sang close code như sau:
      - JWT `Rejected`, ticket `Invalid`, hoặc `device_id` không khớp → `AUTH_FAILED`.
      - Ticket `Expired` → `SESSION_EXPIRED`.
      - `ticket.cell ≠ gateway.cell` → `WRONG_CELL`.
      - Limiter từ chối → `RATE_LIMITED` (`retryAfter` 30000).
      - Mục d–g và i chưa áp dụng ở M1.
    - Đăng nhập bằng JWT: `authAt = now`, `exp = now + sessionTtl`, `authJtiHash = jtiHash(jti)`, `topics` lấy từ claims. Đăng nhập bằng ticket: giữ nguyên `authAt`, `exp`, `authJtiHash`, `topics` của ticket. Ticket mới cấp mang các giá trị đó.
    - Verify JWT qua `JwtAuthFlow` (Task 15). Frame client gửi nối tiếp nằm lại trong socket trong lúc chờ (`autoRead=false`). Frame đã decode nhưng tới trước khi handler bị gỡ thì xếp vào hàng đợi và chuyển cho `SessionHandler` theo đúng thứ tự sau `CONNECTED` (Review Focus 1).

- [ ] **Step 1: Viết test thất bại**

```java
@Test void nonHelloBeforeAuthClosesBadFrame() { }
@Test void helloWithJwtSendsConnected() {
  // conn_id hợp lệ (ConnId.isValid), heartbeat_sec 30, seq_unknown true,
  // session_expires_at_ms == (now + 86400) * 1000, resume ticket verify → Valid; session ACTIVE; PrincipalIndex có principal
}
@Test void helloWithTicketKeepsAuthAtAndExpiry() { }
@Test void deviceMismatchAuthFailed() { }
@Test void ticketFromOtherCellWrongCell() { }
@Test void expiredTicketSessionExpired() { }
@Test void tamperedTicketAuthFailed() { }
@Test void rejectedJwtAuthFailed() { }
@Test void twentyFirstHandshakePerMinuteRateLimited() { /* close 4009, reason chứa retryAfter=30000 */ }
@Test void unknownKidRefreshesThenConnects() { }
@Test void unknownKidStillMissingAfterRefreshAuthFailed() { }
@Test void pipelinedFramesAfterHelloAreProcessedAfterConnected() {
  // HELLO (kid lạ) và PING trong cùng một lần write inbound → outbound theo thứ tự CONNECTED rồi PONG (Review Focus 1)
}
@Test void limiterSlidingWindow() { /* PrincipalHandshakeLimiterTest: 20 ok, 21 fail, +60 s ok */ }
```

Test dùng `EmbeddedChannel` với pipeline `FrameCodec → HelloAuthHandler → SessionHandler → ErrorTerminator` và bật sẵn `PROTOCOL_READY`.

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-service test -Dtest='HelloAuthHandlerTest,PrincipalHandshakeLimiterTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực**

- [ ] **Step 4: Chạy để thấy pass**

Run: `./mvnw -f gateway-service test -Dtest='HelloAuthHandlerTest,PrincipalHandshakeLimiterTest'`
Expected: mọi test pass.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): HELLO authentication with JWT, resume tickets and async JWKS refresh"
```

---

### Task 17: TLS, `UpgradeGuard` và channel initializer

**Files:**
- Create: `pipeline/{SslContextProvider,UpgradeGuard,WsChannelInitializer,TcpChannelInitializer}.java`
- Test: `src/test/java/com/pangate/gateway/pipeline/{SslContextProviderTest,UpgradeGuardTest}.java`

**Interfaces:**
- Consumes: Task 7 (sinh cert dev cho test), 10, 14, 15, 16 (`SessionHandler`, `HelloAuthHandler`).
- Produces:
  - `SslContextProvider(GatewayProperties.Tls tls)`: `SslContext ws()` (ALPN `http/1.1`) và `SslContext tcp()` (ALPN `pangate.v1`, `SelectorFailureBehavior.NO_ADVERTISE`). Cả hai dùng `SslProvider.OPENSSL_REFCNT` (fallback `JDK` kèm WARN nếu `OpenSsl.isAvailable()` sai) và chỉ bật `TLSv1.3`. Thiếu file → `SecretNotFoundException`.
  - `UpgradeGuard(GatewayProperties p, LoopRegistry loops, NodeState node, RandomGenerator rnd)`, mỗi channel một instance:
    - Path khác `/v1/ws` → 404 rồi close.
    - Thiếu `pangate.v1` trong `Sec-WebSocket-Protocol` → 426.
    - `loops.totalActive() ≥ softCap` hoặc đang `draining` → 503 với `Retry-After` ngẫu nhiên trong [5, 30].
    - Hợp lệ → `session.origin(req.headers().get(ORIGIN))`, `fireChannelRead(req.retain())`, rồi tự gỡ khỏi pipeline.
  - `WsChannelInitializer`: dựng pipeline đúng thứ tự spec §4.1, trừ `HAProxyMessageDecoder` (M3):

    `AcceptGate → SslHandler → FlushConsolidationHandler(256, true) → HttpServerCodec → HttpObjectAggregator(8192) → UpgradeGuard → WebSocketServerProtocolHandler(config: path /v1/ws, subprotocols pangate.v1, maxFramePayloadLength 65536, allowExtensions false, handleCloseFrames false, handshakeTimeoutMillis 10000) → WsBinaryAdapter → FrameCodec → HelloAuthHandler → SessionHandler → ErrorTerminator`.

    Đặt attr `TRANSPORT = WS`.
  - `TcpChannelInitializer`: `AcceptGate → SslHandler → FlushConsolidationHandler → LengthFieldBasedFrameDecoder(66560, 0, 4, 0, 4) → LengthFieldPrepender(4) → FrameCodec → HelloAuthHandler → SessionHandler → ErrorTerminator`. Đặt attr `TRANSPORT = TCP`.

- [ ] **Step 1: Viết test thất bại**

```java
@Test void loadsDevCertificateWithTls13Only(@TempDir Path d) { /* DevSecretsGenerator.ensure(d) → ws().newEngine(ALLOC).getEnabledProtocols() == ["TLSv1.3"] */ }
@Test void tcpContextAdvertisesPangateAlpn() { }
@Test void missingCertFileThrowsSecretNotFound() { }
@Test void wrongPathIs404() { }
@Test void missingSubprotocolIs426() { }
@Test void aboveSoftCapIs503WithRetryAfterBetween5And30() { }
@Test void validUpgradePassesThroughAndRemovesGuard() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-service test -Dtest='SslContextProviderTest,UpgradeGuardTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực**

- [ ] **Step 4: Chạy để thấy pass**

Run: `./mvnw -f gateway-service test -Dtest='SslContextProviderTest,UpgradeGuardTest'`
Expected: mọi test pass.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): TLS 1.3 contexts, upgrade guard and WS/TCP pipelines"
```

---

### Task 18: `NettyGatewayServer`, cổng quản trị, metrics, IT end-to-end

**Files:**
- Create: `server/NettyGatewayServer.java`, `mgmt/{ReadinessCheck,GatewayReadinessHealthIndicator,EventLoopLagHealthIndicator,AdminController}.java`, `metrics/EventLoopLagProbe.java`, `tools/HealthCheck.java`, `config/GatewayConfig.java` (wiring toàn bộ bean)
- Test: `src/test/java/com/pangate/gateway/metrics/EventLoopLagProbeTest.java`, `mgmt/HealthIndicatorsTest.java`, `mgmt/AdminControllerTest.java`; `src/it/java/com/pangate/gateway/it/GatewayLocalIT.java`

**Interfaces:**
- Consumes: mọi task trước; `GatewayClient`, `Hellos`, `JwtMinter`, `DevKeys` từ devtools (scope test).
- Produces:
  - `NettyGatewayServer implements SmartLifecycle` (phase `Integer.MAX_VALUE - 100`):
    - `start()`:
      1. `loops.all().forEach(LoopContext::startTicker)`.
      2. Bind `effectiveAcceptors` listener WS (EPOLL bật `SO_REUSEPORT`) và một listener TCP nếu `enabled`. Option chung: `SO_BACKLOG 65535`, `TCP_NODELAY true`, `SO_KEEPALIVE false`, `RCVBUF_ALLOCATOR AdaptiveRecvByteBufAllocator(64, 1024, 65536)`, `WRITE_BUFFER_WATER_MARK(32 KB, 64 KB)`. Bind lỗi → `ListenerBindException` (envVar `GATEWAY_LISTENERS_WS_PORT` hoặc `GATEWAY_LISTENERS_TCP_PORT`).
      3. `node.listenersBound = true`, `admission.markReady()`.
    - `stop(Runnable cb)`:
      1. `node.draining = true`, close các listener.
      2. Trên mỗi loop: `execute(forEachSession(s -> s.close(GOING_AWAY, "SHUTDOWN")))`, chờ tối đa 5 s.
      3. `io.shutdown(0, 5 s)` rồi gọi `cb`.
  - `interface ReadinessCheck { Optional<String> notReadyReason(); }`. M1 có 3 bean check: listener đã bind, không draining, `totalActive < softCap`. M2 và M3 thêm check bằng cách khai báo thêm bean.
  - `@Component("gatewayReadiness") GatewayReadinessHealthIndicator implements HealthIndicator`: `UP` khi mọi `ReadinessCheck` trả rỗng; ngược lại `OUT_OF_SERVICE` với `details.reasons` là danh sách lý do. Tên bean phải là `gatewayReadiness` để khớp `include` của group `readiness`.
  - `@Component("eventLoopLag") EventLoopLagHealthIndicator`: `UP` khi `lagProbe.maxP99Seconds() < 5`, ngược lại `DOWN` với detail `maxP99Seconds`.
  - `AdminController` (`@RestController`): `POST /admin/drain` trả 403 nếu `request.getRemoteAddr()` không phải loopback. Ở M1 (chưa có `DrainCoordinator`) trả 202 và gọi `NettyGatewayServer.stop()` bất đồng bộ; M3 thay bằng drain thật.
  - `EventLoopLagProbe(LoopRegistry, TickScheduler, LongSupplier nanoClock)`: mỗi 100 ms, mỗi loop được `execute` một no-op để đo độ trễ. Lưu 100 mẫu gần nhất của mỗi loop vào `AtomicLongArray`. Method `double p99Seconds(int loop)`, `double maxP99Seconds()`. Gauge `gw.eventloop.lag{loop}` = p99.
  - `HealthCheck.main(String[] args)`: GET `args[0]` với timeout 2 s, `System.exit(0)` nếu 200, ngược lại `exit(1)`.
  - `GatewayConfig`: khai báo bean cho `IoGroups`, `LoopRegistry` (wheel 128 slot, đồng hồ `System.currentTimeMillis()/1000`), `LimitsHolder`, `NodeState`, `TickScheduler`, `AdmissionController`, `IpConnectionCounter`, `PrincipalIndex`, `SessionTracker`, `TicketKeyRing`, `ResumeTicketService`, `JwksCache` (start/stop theo lifecycle bean), `JwtVerifier`, `PrincipalHandshakeLimiter`, `SslContextProvider`, `GatewayMetrics`, `EventLoopLagProbe`, hai initializer, offload executor là virtual thread.

- [ ] **Step 1: Viết unit test thất bại**

```java
@Test void lagProbeReportsDelayOfBlockedLoop() { /* group 1 thread đặt tên "test-io" (không thuộc gw-io-* nên BlockHound cho phép sleep), chặn loop 300 ms → p99Seconds(0) ≥ 0.25 */ }
@Test void readinessIndicatorListsReasons() { /* check trả "listeners not bound" → Health OUT_OF_SERVICE, details.reasons chứa lý do; bỏ check → UP */ }
@Test void livenessIndicatorDownWhenLagOver5s() { }
@Test void drainRejectsNonLoopback() { /* MockMvc với remoteAddr 10.0.0.5 → 403; 127.0.0.1 → 202 */ }
```

- [ ] **Step 2: Viết IT thất bại `GatewayLocalIT`**

`@SpringBootTest(classes = GatewayApplication.class)`, `@ActiveProfiles("local")`. `@DynamicPropertySource` đặt cổng trống (ws, tcp, `server.port`), `gateway.dev.dir` = thư mục `@TempDir` tĩnh, `gateway.io.threads=4`. Token được mint bằng `JwtMinter` với key đọc qua `DevKeys.load(devDir/es256.pem)`.

```java
@Test void becomesReadyWithin30s() { /* poll /ready ≤ 30 s → 200 {"status":"UP"}; /live → 200; /actuator/prometheus chứa "gw_handshakes_total" */ }
@Test void managementPortServesBeforeNettyAndDuringShutdown() { /* trong lúc NettyGatewayServer.stop() đang chạy, GET /ready trả 503 (draining) chứ không bị từ chối kết nối */ }
@Test void jwtHelloThenPingOverWss() { /* ClientTls.trust(ca.crt); CONNECTED.seq_unknown; ping → PONG */ }
@Test void ticketReconnectKeepsExpiry() { }
@Test void tcpListenerAcceptsHello() { /* tcps:// với ALPN pangate.v1 */ }
@Test void pipelinedHelloAndPingWithRotatedKey() {
  // sinh key ES256 mới, ghi đè jwks.json (giữ cả key cũ lẫn key mới), mint bằng key mới,
  // send(HELLO) + send(PING) không chờ → nhận CONNECTED rồi PONG (Review Focus 1)
}
@Test void abruptDisconnectsLeaveNoTrackedState() {
  // 50 socket TCP thô đóng trước TLS; 10 client abort() sau CONNECTED; 5 client đóng giữa upgrade
  // → await ≤ 10 s: loops.totalActive()==0, ipCounter.trackedKeys()==0, principalIndex.principalCount()==0 (Review Focus 2)
}
@Test void wrongSubprotocolGets426() { }
@Test void shutdownSendsGoingAway() { /* client đang mở; context.close() → client.awaitClose().code()==1001 */ }
@Test void restartKeepsTicketsValidAndRebindsImmediately() {
  // context 1: connect, lưu ticket; close context (đo ≤ 15 s); start context 2 ngay trên cùng port
  // → ready ≤ 30 s; HELLO bằng ticket cũ → CONNECTED (Review Focus 4, R3)
}
```

Hai test cuối tự tạo `SpringApplicationBuilder` riêng thay vì dùng context dùng chung của class.

- [ ] **Step 3: Chạy để thấy thất bại**

Run: `./mvnw -f gateway-service verify`
Expected: unit test mới fail hoặc lỗi compile; `GatewayLocalIT` fail.

- [ ] **Step 4: Hiện thực `NettyGatewayServer`, `ReadinessCheck` + 2 health indicator, `AdminController`, `EventLoopLagProbe`, `HealthCheck`, `GatewayConfig`**

- [ ] **Step 5: Chạy để thấy pass**

Run: `./mvnw -f gateway-service verify`
Expected: toàn bộ unit test và `GatewayLocalIT` pass; BlockHound không báo lỗi.

- [ ] **Step 6: Kiểm tra chạy local từ dòng lệnh**

Run: `make bootstrap && (./mvnw -f gateway-service spring-boot:run -Dspring-boot.run.profiles=local &) && sleep 25 && curl -s localhost:8081/ready`
Expected: `{"status":"UP"}`; thư mục `~/.pangate-dev` có đủ file. Dừng process (Ctrl-C hoặc `kill`) và thấy thoát trong ≤ 15 s.

- [ ] **Step 7: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): netty server lifecycle, management port, lag probe and local e2e IT"
```

---

### Task 19: Image, compose dev, Makefile, script kiểm tra

**Files:**
- Modify: `gateway-service/pom.xml`, `gateway-devtools/pom.xml` (cấu hình Jib), `Makefile`
- Create: `platform-devenv/docker-compose.yml`, `platform-devenv/README.md`, `platform-devenv/scripts/local-start-check.sh`, `platform-devenv/scripts/idle-memory.sh`

**Interfaces:**
- Consumes: jar và các main class: `com.pangate.gateway.GatewayApplication`, `com.pangate.gateway.tools.HealthCheck`, `com.pangate.devtools.Main`.
- Produces:
  - Image `pangate/gateway-service:dev` và `pangate/gateway-devtools:dev`: Jib `dockerBuild`, base `gcr.io/distroless/java21-debian12:nonroot`, có sẵn `/app/jib-classpath-file`.
  - Compose (project name `pangate`), volume `dev-secrets`:

    | Service | Profile | Lệnh / cấu hình | Healthcheck |
    |---|---|---|---|
    | `issuer` | `app` | `user: "0"`, mount `dev-secrets:/dev-secrets`, `issuer --port 8090 --key /dev-secrets/es256.pem` | `["CMD","java","-cp","@/app/jib-classpath-file","com.pangate.devtools.Main","health","http://127.0.0.1:8090/.well-known/jwks.json"]` (interval 5 s, retries 20) |
    | `gateway` | `app` | `user: "0"`, `SPRING_PROFILES_ACTIVE=dev`, `JAVA_TOOL_OPTIONS=-Xmx1g -XX:MaxDirectMemorySize=1g`, ports `8443:8443`, `8883:8883`, `8081:8081`, `depends_on: issuer: condition: service_healthy` | `HealthCheck` gọi `http://127.0.0.1:8081/ready` |
    | `devtools` | `tools` | mount `dev-secrets:/dev-secrets:ro`, không có lệnh mặc định | — |

  - Make target:

    | Target | Lệnh |
    |---|---|
    | `doctor` | Kiểm `java -version` 21, `docker compose version`, các port 8443, 8883, 8081 còn trống (`ss -ltn`) |
    | `run-local` | `./mvnw -f gateway-service spring-boot:run -Dspring-boot.run.profiles=local` |
    | `images` | `./mvnw -f gateway-devtools jib:dockerBuild` và `./mvnw -f gateway-service jib:dockerBuild` (`-Djib.to.image=…:dev`) |
    | `up` | `docker compose -f platform-devenv/docker-compose.yml --profile app up -d --wait --wait-timeout 90` |
    | `down` | `docker compose … --profile app --profile tools down -v` |
    | `logs` | `docker compose … logs -f` |
    | `smoke` | `docker compose … --profile tools run --rm devtools smoke --url wss://gateway:8443/v1/ws --issuer http://issuer:8090 --ca /dev-secrets/ca.crt` |

  - `local-start-check.sh <gateway-jar> <devtools-exec-jar>` (bash, `set -euo pipefail`):
    1. Đặt `GATEWAY_DEV_DIR=$(mktemp -d)` và khởi động jar với profile `local`.
    2. Poll `/ready` ≤ 30 s.
    3. Mint token bằng `devtools token --key $GATEWAY_DEV_DIR/es256.pem …`, chạy `devtools client --once --ca …`.
    4. `kill -TERM`, kiểm process thoát trong ≤ 15 s.
    5. Chạy lại, ready trong ≤ 30 s, rồi dừng.

    Bước nào lỗi thì in log của gateway và exit khác 0.
  - `idle-memory.sh <gateway-jar> <devtools-exec-jar> [N=10000]`:
    1. Khởi động gateway profile `local`, chờ ready.
    2. `jcmd <pid> GC.run`, đọc baseline `jvm_memory_used_bytes{area="heap"}` (tổng mọi pool) từ `/actuator/prometheus`.
    3. Chạy `devtools client --load N --ramp 2000` (bind `127.0.0.1`) trong process riêng, chờ đủ N kết nối.
    4. `GC.run`, đo lại heap. In `heap_per_session_bytes`; exit 1 nếu ≥ 3072.

- [ ] **Step 1: Cấu hình Jib, compose, Makefile và script**

- [ ] **Step 2: Kiểm tra môi trường dev**

Run: `make bootstrap images up smoke`
Expected: `docker compose ps` cho thấy `issuer` và `gateway` đều `healthy` trong ≤ 90 s; `smoke` in ra `SMOKE OK` và exit 0.

- [ ] **Step 3: Kiểm tra script local**

Run: `platform-devenv/scripts/local-start-check.sh gateway-service/target/gateway-service-0.1.0-SNAPSHOT.jar gateway-devtools/target/gateway-devtools-0.1.0-SNAPSHOT-exec.jar`
Expected: in `LOCAL START OK`, exit 0.

Run: `platform-devenv/scripts/idle-memory.sh gateway-service/target/gateway-service-0.1.0-SNAPSHOT.jar gateway-devtools/target/gateway-devtools-0.1.0-SNAPSHOT-exec.jar 10000`
Expected: `heap_per_session_bytes` < 3072, exit 0. Nếu ≥ 3072, ghi con số đo được vào `docs/superpowers/plans/2026-11-03-gateway-m1-single-node.md` mục Kết quả đo, rồi báo lại để quyết định trước khi sửa ngưỡng trong spec.

- [ ] **Step 4: `make down` rồi commit**

```bash
make down
git add Makefile platform-devenv gateway-service/pom.xml gateway-devtools/pom.xml
git commit -m "build: jib images, dev compose, make targets and local start/memory checks"
```

---

### Task 20: CI và kiểm tra tiêu chí hoàn thành M1

**Files:**
- Create: `.github/workflows/ci.yml`
- Modify: `README.md` (thêm phần "Chạy local" và "Chạy dev"), `gateway-service/README.md` (cách chạy từ IntelliJ và VS Code: main class `com.pangate.gateway.GatewayApplication`, profile `local`)

**Interfaces:**
- Produces: workflow chạy trên `push` và `pull_request`, gồm 4 job trên `ubuntu-latest` với `actions/setup-java` (temurin 21, cache maven):

  | Job | Việc |
  |---|---|
  | `build` | `make verify` (mọi module, gồm IT) |
  | `contracts-lint` | `bufbuild/buf-action@v1` với `input: platform-contracts`, `lint: true`, `breaking: false` |
  | `local-start` | `needs: build`; `make bootstrap`, rồi `local-start-check.sh` và `idle-memory.sh` (N=10000) |
  | `dev-start` | `needs: build`; `make bootstrap images up smoke`, `make down` luôn chạy (`if: always()`) |

- [ ] **Step 1: Viết `ci.yml` và cập nhật README**

- [ ] **Step 2: Kiểm tra cú pháp workflow**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml'))" && echo ok`
Expected: `ok`.

- [ ] **Step 3: Chạy lại toàn bộ tiêu chí hoàn thành M1 trên máy**

Run lần lượt:
- `make verify`: mọi test pass.
- `platform-devenv/scripts/local-start-check.sh …`: `LOCAL START OK`.
- `make images up smoke down`: `SMOKE OK`.
- Kiểm tra 50k session idle (spec §12):
  1. Chạy `ulimit -n 200000`.
  2. Khởi động gateway bằng `make run-local`.
  3. Chạy `java -jar gateway-devtools/target/gateway-devtools-0.1.0-SNAPSHOT-exec.jar client --url wss://127.0.0.1:8443/v1/ws --ca ~/.pangate-dev/ca.crt --token "$(… token …)" --device load --load 50000 --ramp 2000 --bind 127.0.0.1,127.0.0.2`.
  4. Chờ client in `connected=50000`.
  5. Đọc `gw_eventloop_lag{loop=…}` trên `/actuator/prometheus`.

Expected: mọi bước thành công; `connected=50000`; mọi giá trị `gw_eventloop_lag` < 0.020. Ghi kết quả vào cuối plan, mục "Kết quả đo M1".

- [ ] **Step 4: Commit và push**

```bash
git add .github README.md gateway-service/README.md docs/superpowers/plans
git commit -m "ci: build, contracts lint, local-start and dev-start checks"
git push -u origin claude/fervent-turing-vz339r
```

---

## Kết quả đo M1

| Chỉ số | Kết quả | Ngưỡng | Ghi chú |
|---|---|---|---|
| Ready ở local (`java -jar`, profile `local`) | 4 s | ≤ 30 s | Đạt |
| Thoát sau SIGTERM | 1,6 s | ≤ 15 s | Đạt |
| `make up` tới khi mọi container healthy | 12 s | ≤ 90 s | Đạt; `make smoke` → `SMOKE OK` |
| Heap mỗi session idle (10k session, WSS + TLS) | **5.364 B** | < 3.072 B | **Chưa đạt**, chờ quyết định |

Phân bổ heap mỗi session (class histogram, 10k session): 10 `DefaultChannelHandlerContext` 640 B; 17 `String` + `byte[]` khoảng 1 KB (connId, principal, device, tenant, platform, sdkVersion, origin, khóa index); `EpollSocketChannel` 152 B; `ReferenceCountedOpenSslEngine` + session 224 B; `SslHandler` + promise 208 B; `Session` 136 B; attribute 120 B; phần còn lại là map và config.

