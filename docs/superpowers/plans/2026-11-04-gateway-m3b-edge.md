# Gateway M3b (TLS reload, SSE, PROXY, tenant cap, trace) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hoàn thiện phần "biên" của gateway cho M3:
- Cert TLS và key được thay khi đang chạy, không cần restart: cert, TLS session ticket key, resume ticket key.
- Fallback SSE `POST /v1/sse` và `POST /v1/upstream` qua HTTP.
- PROXY protocol, để thấy IP thật của client sau LB.
- Tenant cap: quota local cộng phần mượn qua Redis.
- Kill switch `trace.principal` và span OTel `gw.push.handoff` / `gw.push.write`.

**Architecture:**
- **Key và cert:** đọc lại định kỳ trên offload executor; `gw-tick` chỉ lên lịch. Bản mới được swap qua `AtomicReference` hoặc field volatile. Bản mới đọc lỗi thì giữ bản cũ.
- **Định tuyến HTTP:** chỉ đổi `UpgradeGuard`. Theo path, nó thay phần đuôi pipeline WS bằng `SseAdapter` (rồi `FrameCodec → HelloAuthHandler → SessionHandler` như WS) hoặc bằng `HttpUpstreamHandler`.
- **PROXY protocol:** `ProxyProtocolHandler` giữ `channelActive` lại cho tới khi đọc xong header, nên `AcceptGate` thấy địa chỉ thật.
- **Tenant cap:** đếm local theo node. Phần mượn thêm chạy Lua trên Redis state, mỗi node ghi tổng của mình kèm hạn 30 s, để entry của node chết tự hết hiệu lực.
- **Trace:** cờ `traced` trên `Session` bật log chi tiết trong `FrameCodec`. Span OTel chỉ được tạo trong `PushDelivery`.

**Tech Stack:** Như M3a, thêm:
- `io.netty:netty-codec-haproxy`, version theo Netty BOM 4.2.20.
- `io.opentelemetry:opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-otlp` (sender JDK `opentelemetry-exporter-sender-jdk`, loại `opentelemetry-exporter-sender-okhttp`), và `opentelemetry-sdk-testing` (scope test). Tất cả version 1.49.0, theo BOM của Spring Boot 3.5.16.

**Spec:** `docs/superpowers/specs/2026-11-03-gateway-service-design.md`. Plan này là phần giữa của **M3** (§12): M3a bảo vệ và vòng đời (xong); M3b phần biên; M3c fuzz, JMH, image prod, ADR, release. Các mục chính: §4.1, §4.3, §4.6 bước i, §4.7, §5.10 bước 4, §7.7, §7.8, §8, §9, §10.

## Global Constraints

**Kế thừa từ M1–M3a**
- Không block trên `gw-io-*` hay `gw-tick`. File I/O và Redis chạy trên offload executor hoặc dạng async; kết quả quay về owner loop qua `execute`.
- Mọi thay đổi `Session` chạy trên owner loop. Test bật `-Dpangate.assertLoop=true` và BlockHound.
- Metric đặt tên dạng chấm, label hữu hạn, **không** gắn label theo principal hay kết nối. Conventional Commits.
- Chạy impact trước khi sửa symbol, `detect-changes` trước khi commit.
- Heap/session < 5120 B (M3a đo 4777 B). M3b chỉ được thêm vào `Session` **2 field `boolean`**: `traced` và `tenantReserved`.

**TLS** (spec §7.7)
- `gateway.tls.reload-interval` mặc định 30 s. Thay đổi được phát hiện bằng `(lastModified, size)` của cả cert và key.
- Context mới: `SslProvider.OPENSSL_REFCNT`, TLS 1.3, ALPN như M1. Context cũ được `release()` sau 60 s.
- Session ticket key: secret `tls-ticket-keys/{prev,current,next}`. Mỗi file là một dòng base64 của 48 byte, chia thành `name(16) | hmac(16) | aes(16)`. Nạp bằng `OpenSslSessionContext.setTicketKeys(current, next, prev)`: key đầu tiên dùng để mã hóa. Đọc lại mỗi 60 s, và nạp lại mỗi khi dựng context mới.

**Resume ticket key** (spec §4.7): đọc lại `ticket-keys/{prev,current,next}` mỗi 60 s. Đọc lỗi hoặc định dạng sai thì giữ ring cũ, ghi WARN và tăng `gw.secrets.reload{name,result=failed}`.

**PROXY protocol** (spec §4.1, §9)
- Cấu hình `gateway.listeners.proxy-protocol: { enabled: false, trusted-cidrs: [] }`.
- Chỉ peer thuộc `trusted-cidrs` mới phải gửi PROXY header (v1 hoặc v2). Peer ngoài danh sách và peer loopback (warm-up, health check cục bộ) được coi là kết nối trực tiếp.
- Chờ header tối đa `limits.hello-timeout` (5 s), quá hạn thì đóng.
- Lệnh `LOCAL` (health check của LB) giữ địa chỉ socket.
- Địa chỉ trong header chỉ dùng cho admission (per-IP), `Session.remoteAddress` và log. Kiểm tra "chỉ từ loopback" của warm-up luôn dùng **địa chỉ socket**.

**Tenant cap** (spec §4.6 bước i, §10)
- Chỉ áp dụng khi `maxConns > 0`.
- `quota = ceil(maxConns × 1.1 / nodesInCell)`, với `nodesInCell = max(1, ZCOUNT nodes:live (now − 30 s) +inf)`, đọc lại mỗi 10 s.
- Còn quota local thì nhận ngay. Hết quota thì mượn qua Redis state: Lua cộng tổng còn hạn của các node khác với tổng mới của node này. Kết quả > `maxConns`, Redis lỗi hoặc quá 200 ms → close 4013 (`OVERLOADED`, `retry_after_ms` ngẫu nhiên 5000–30000).
- Mỗi 10 s mỗi node ghi tổng của mình cho mọi tenant có cap, hạn 30 s.

**SSE và upstream HTTP** (spec §4.3)
- `POST /v1/sse`, `content-type: application/x-protobuf`, body là `Hello`. Response `200 text/event-stream`, chunked.
  - Mỗi frame gửi xuống là một event `data: <base64 Frame>\n\n`.
  - Mỗi 30 s gửi một dòng comment `:\n\n`; ghi thành công tính là dấu hiệu sống (`Session.touch`).
  - Tenant chưa bật `sse.fallback.enabled` → 401.
- `POST /v1/upstream`, header `Authorization: Ticket <resume>`, body là `Frame(UPSTREAM)`.
  - Mỗi kết nối chỉ một request; response có `Connection: close`.
  - `202`: đã nhận. Với `cmd` và `cmd_result`, chỉ trả sau khi Kafka ack.
  - `400`: body hỏng, type lạ hoặc thiếu `msg_id`.
  - `401`: ticket sai hoặc hết hạn.
  - `403`: revoked hoặc sai cell.
  - `405`: method không phải POST.
  - `413`: body > `frame-max`.
  - `415`: content-type sai.
  - `429` kèm `Retry-After: 300`: tenant bị block.
  - `503`: Kafka không nhận.
- Path không phải `/v1/ws` thì không cần subprotocol. Các luật 503 của `UpgradeGuard` (draining, soft cap, shed ≥ 3) áp dụng cho mọi path.

**Trace** (spec §7.8, §8)
- Control key `trace:<principal>` → `ControlEvent.trace = TracePrincipal{principal_id, expires_at_ms}`. Tombstone thì gỡ.
- Hạn hiệu lực `= min(expires_at_ms nếu > 0, issued_at_ms + 15 phút)`.
- Session của principal đó: logger `pangate.trace` ghi ở INFO lúc activation, mỗi frame vào và ra (type, `msg_id`, số byte) và lúc close. Mọi push của session đó đều có span.
- Principal trong log luôn được hash: `HMAC-SHA256(principal-hash.key, principal)`, lấy 8 byte đầu, dạng hex.
- Span: `gw.push.handoff` (từ `enqueued_at_ms` tới lúc gọi write) và `gw.push.write` (từ write tới lúc write promise hoàn tất).
  - Chỉ tạo khi `frame.headers.traceparent` có cờ sampled, hoặc session đang `traced`.
  - Riêng `gw.push.handoff` còn được tạo (status ERROR, attribute `pangate.nack`) khi push bị NACK, kể cả khi không sampled.
  - Exporter OTLP/HTTP tới `gateway.tracing.otlp-endpoint`; rỗng (mặc định) thì dùng tracer no-op.

**Metric mới**
- `gw.tls.reloads{result=ok|failed}`, `gw.secrets.reload{name,result}`.
- `gw.admission.rejected{reason=proxy}`.
- `gw.tenant.borrow{result=ok|denied|error}`, `gw.handshakes{result=tenant_cap|sse_disabled}`.
- `gw.http.upstream{status}`, `gw.sse.sessions`.

## Review Focus

1. **File cert bị thay dở dang, hoặc cặp cert/key không khớp.** Kết nối mới vẫn dùng context cũ; WARN; `gw.tls.reloads{result=failed}`; lần kiểm sau thử lại. Test: Task 1 `brokenReplacementKeepsCurrentContext`.
2. **Có người khai địa chỉ giả.** Có hai trường hợp:
   - PROXY header khai `127.0.0.1`: không được thỏa điều kiện "warm-up chỉ từ loopback".
   - Peer không thuộc `trusted-cidrs` gửi PROXY header: header không được dùng; kết nối đi tiếp như trực tiếp và hỏng ở TLS.

   Test: Task 3 `proxyClaimedLoopbackDoesNotUnlockWarmUp`, `untrustedPeerHeaderIsNotParsed`.
3. **Node bị SIGKILL trong khi đang giữ phần đếm của tenant.** Entry của nó hết hạn sau ≤ 30 s, nên tenant không bị khóa vĩnh viễn ở mức cap. Test: Task 4 `expiredNodeEntriesAreIgnored` (IT).
4. **Client SSE mất mạng không có FIN, hoặc nhận push RELIABLE.**
   - Comment heartbeat vẫn ghi; nếu client ngừng đọc thì slow consumer đóng 4008; session không bị rò.
   - RELIABLE trên SSE được trả `STATUS_WRITTEN` ngay khi ghi, nên không làm đầy pending window.

   Test: Task 6 `reliablePushOnSseIsAckedWritten`, `heartbeatCommentTouchesSession`.
5. **Rotate resume ticket key qua ngày.** Ticket ký bằng `current` cũ vẫn verify được sau khi key đó thành `prev`; file key mới đọc lỗi thì giữ ring cũ. Test: Task 2 `ticketFromPreviousCurrentVerifiesAfterRotation`, `unreadableRingKeepsOld`.

---

## File Structure

```
gateway-service/src/main/java/com/pangate/gateway/
├── config/    GatewayProperties (+Tls.reloadInterval, Listeners.proxyProtocol, Tracing), EdgeConfig (mới: reload, tenant cap, tracing)
├── pipeline/  SslContextProvider (reload), TlsTicketKeys (mới), ProxyProtocolHandler (mới), UpgradeGuard (định tuyến),
│              SseAdapter (mới), HttpUpstreamHandler (mới), AcceptGate (địa chỉ thật), Ws/TcpChannelInitializer,
│              HelloAuthHandler (tenant cap, SSE, traced)
├── net/       CidrSet (mới)
├── auth/      ResumeTicketService (ring volatile), TicketKeyRing (equals), HelloPolicy (+checkTicketUse)
├── tenant/    TenantConnCounter, TenantCapBorrower, CellSize (mới)
├── trace/     TraceTargets, PrincipalHasher, SessionTrace, GatewayTracing (mới)
├── control/   ControlEventConsumer/ControlKeys (key trace:), ControlEnforcer (cờ traced)
├── protocol/  ChannelAttrs (+CLIENT_ADDRESS, socketPeer/clientAddress, TransportKind.SSE), ChannelCloser (SSE), FrameCodec (log trace)
├── session/   Session (+traced, +tenantReserved), SessionTracker (nhả quota tenant)
├── upstream/  UpstreamPublisher (+publishHttp)
├── downstream/ PushDelivery (span, SSE ack-on-write)
└── dev/       DevSecretsGenerator (+tls-ticket-keys, +reissueGatewayCert)
platform-contracts/proto/pangate/v1/control.proto   (+TracePrincipal)
gateway-devtools/.../client/GatewayClient.java        (+proxyHeader)
gateway-service/src/it/java/com/pangate/gateway/it/GatewayEdgeIT.java
```

---

### Task 1: TLS hot-reload (`SslContextProvider`)

**Files:**
- Modify: `config/GatewayProperties.java` (`Tls` thêm `Duration reloadInterval`), `pipeline/SslContextProvider.java`, `application.yml` (`tls.reload-interval: 30s`), `config/GatewayConfig.java`
- Create: `config/EdgeConfig.java`
- Test: `pipeline/SslContextProviderTest.java`, `config/GatewayPropertiesTest.java`

**Interfaces:**
- Produces:
  ```java
  public final class SslContextProvider {
    public SslContextProvider(GatewayProperties.Tls tls, ScheduledExecutorService releaser, Duration releaseDelay, GatewayMetrics m);
    public SslContext ws();                 // context hiện hành, đọc volatile
    public SslContext tcp();
    public boolean reloadIfChanged();       // offload executor: (mtime,size) đổi → dựng cả hai; lỗi → giữ cũ, false
    public void onContextsBuilt(java.util.function.Consumer<SslContext> hook); // Task 2 nạp ticket key vào context mới
  }
  ```
  - Hai context được giữ chung trong một `record Contexts(SslContext ws, SslContext tcp, long certMtime, long certSize, long keyMtime, long keySize)` sau `AtomicReference`.
  - Context cũ được `ReferenceCountUtil.release` sau `releaseDelay` (prod 60 s) trên `releaser`. Context JDK thì bỏ qua.
- `EdgeConfig` lên lịch `tick.scheduleAtFixedRate(() -> offload.execute(provider::reloadIfChanged), tls.reloadInterval())`.

- [ ] **Step 1: Viết test thất bại**

```java
// SslContextProviderTest — dir tạm, DevSecretsGenerator.ensure(dir) để có cert/key; releaseDelay = ZERO, releaser = Runnable::run style
@Test void unchangedFilesDoNotRebuild() { assertThat(p.reloadIfChanged()).isFalse(); assertThat(p.ws()).isSameAs(before); }
@Test void replacedCertIsServedToNewConnections() {
  /* DevSecretsGenerator.reissueGatewayCert(dir); setLastModified(+2 s) → reloadIfChanged() true; ws() != before; tcp() != beforeTcp;
     handshake thử qua SSLEngine của p.ws() thấy serial mới; before.refCnt() == 0 nếu là ReferenceCounted;
     meters gw.tls.reloads{result=ok} == 1 */ }
@Test void brokenReplacementKeepsCurrentContext() {
  /* ghi đè gateway.key bằng "garbage" → reloadIfChanged() false; ws() isSameAs before; gw.tls.reloads{result=failed} == 1;
     sửa lại key đúng → lần sau true */ }
// GatewayPropertiesTest
@Test void tlsReloadIntervalDefaults30s() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service test -Dtest='SslContextProviderTest,GatewayPropertiesTest'`
Expected: lỗi compile (`reissueGatewayCert`, constructor mới).

- [ ] **Step 3: Hiện thực theo Interfaces**
- `DevSecretsGenerator.reissueGatewayCert(Path dir)` (public, chỉ dùng cho dev và test): ký lại `gateway.crt`/`gateway.key` bằng `ca.key` hiện có, serial mới.
- Ghi file mới ra file tạm rồi `ATOMIC_MOVE`, để process đang chạy không đọc phải file viết dở.

- [ ] **Step 4: Chạy toàn bộ unit**

Run: `./mvnw -q -f gateway-service test`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): reload TLS certificate without restart"
```

---

### Task 2: Rotate key — TLS session ticket và resume ticket

**Files:**
- Create: `pipeline/TlsTicketKeys.java`
- Modify: `auth/ResumeTicketService.java`, `auth/TicketKeyRing.java`, `dev/DevSecretsGenerator.java`, `config/EdgeConfig.java`, `config/GatewayConfig.java`
- Test: `pipeline/TlsTicketKeysTest.java`, `auth/ResumeTicketServiceTest.java` (bổ sung), `dev/DevSecretsGeneratorTest.java` (bổ sung)

**Interfaces:**
- Consumes: `SslContextProvider.onContextsBuilt` (Task 1).
- Produces:
  ```java
  public final class TlsTicketKeys {
    public static TlsTicketKeys load(SecretSource s);   // 3 file base64 48 B; sai độ dài → IllegalStateException kèm describe(name)
    public void applyTo(SslContext ctx);                  // OpenSslSessionContext.setTicketKeys(current, next, prev); context JDK → bỏ qua
    @Override public boolean equals(Object o);            // so nội dung byte
  }
  public final class ResumeTicketService {
    public void ring(TicketKeyRing next);                 // swap volatile; ticket đang verify dùng ring đọc được lúc bắt đầu
    public TicketKeyRing ring();
  }
  // TicketKeyRing: + equals/hashCode theo (kid, bytes) của 3 key
  ```
- `EdgeConfig`: mỗi 60 s trên offload, đọc lại cả hai loại key. Nội dung khác thì áp dụng:
  - TLS ticket key: nạp vào cả hai context hiện hành, và vào mọi context dựng sau này qua `onContextsBuilt`.
  - Resume ticket key: gọi `tickets.ring(next)`.

  Đọc lỗi thì tăng `gw.secrets.reload{name=tls-ticket-keys|ticket-keys,result=failed}` và giữ bản cũ.
- Lúc khởi động, thiếu `tls-ticket-keys` thì fail-fast như `ticket-keys` (`SecretNotFoundException`). `DevSecretsGenerator.ensure` sinh 3 file còn thiếu, nên thư mục dev cũ tự có thêm ở lần chạy sau.

- [ ] **Step 1: Viết test thất bại**

```java
// TlsTicketKeysTest
@Test void loadsThreeKeysOf48Bytes() { }
@Test void wrongLengthNamesTheSecret() { /* 32 B → message chứa describe("tls-ticket-keys/current") */ }
@Test void ticketIssuedByOneContextResumesOnAnotherWithTheSameKeys() { assumeTrue(OpenSsl.isAvailable());
  /* hai SslContext server dựng riêng, cùng applyTo(keys); client JDK TLS 1.3 handshake với context 1, rồi với context 2 dùng cùng
     client session cache → lần hai isSessionReused (đây là lý do key dùng chung cả cell) */ }
// ResumeTicketServiceTest
@Test void ticketFromPreviousCurrentVerifiesAfterRotation() {
  /* ring A(prev=1,cur=2,next=3) issue t; ring(B(prev=2,cur=3,next=4)) → verify(t) Valid; ring(C(3,4,5)) → verify(t) Invalid */ }
@Test void unreadableRingKeepsOld() { /* EdgeConfig.reloadTicketKeys với SecretSource ném → ring() isSameAs cũ; metric failed == 1 */ }
// DevSecretsGeneratorTest
@Test void createsTlsTicketKeysAndKeepsExisting() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service test -Dtest='TlsTicketKeysTest,ResumeTicketServiceTest,DevSecretsGeneratorTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực theo Interfaces**

Đặt hàm reload thành `EdgeConfig.reloadTicketKeys(SecretSource, ResumeTicketService, GatewayMetrics)` (package-private static) để test gọi trực tiếp được.

- [ ] **Step 4: Chạy toàn bộ unit**

Run: `./mvnw -q -f gateway-service test`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): rotate TLS session ticket keys and resume ticket keys at runtime"
```

---

### Task 3: PROXY protocol

**Files:**
- Create: `net/CidrSet.java`, `pipeline/ProxyProtocolHandler.java`
- Modify: `pom.xml` (gateway-service: `netty-codec-haproxy`), `config/GatewayProperties.java` (`Listeners` thêm `ProxyProtocol proxyProtocol`, `record ProxyProtocol(boolean enabled, List<String> trustedCidrs, boolean trustLoopback)`; `trustLoopback` mặc định `false`, chỉ dùng cho test, không ghi trong spec), `protocol/ChannelAttrs.java`, `pipeline/AcceptGate.java`, `pipeline/WsChannelInitializer.java`, `pipeline/TcpChannelInitializer.java`, `pipeline/HelloAuthHandler.java`, `application.yml`
- Test: `net/CidrSetTest.java`, `pipeline/ProxyProtocolHandlerTest.java`, `pipeline/WarmUpHelloTest.java` (bổ sung)

**Interfaces:**
- Produces:
  ```java
  public final class CidrSet {                    // IPv4 và IPv6, "10.0.0.0/8", "2001:db8::/32", IP trần = /32 hoặc /128
    public static CidrSet parse(List<String> cidrs);  // sai → IllegalArgumentException nêu entry
    public boolean contains(InetAddress a);
  }
  // ChannelAttrs
  AttributeKey<InetSocketAddress> CLIENT_ADDRESS;   // từ PROXY header; không có thì là địa chỉ socket
  static InetAddress socketPeer(Channel ch);        // địa chỉ socket, non-Inet (EmbeddedChannel) → loopback
  static InetAddress clientAddress(Channel ch);     // CLIENT_ADDRESS nếu có, ngược lại socketPeer
  public final class ProxyProtocolHandler extends ChannelInboundHandlerAdapter {   // không Sharable, một bản mỗi channel
    public ProxyProtocolHandler(CidrSet trusted, boolean trustLoopback, Duration headerTimeout, GatewayMetrics m);
  }
  ```
  - `channelActive`:
    - Peer ngoài `trusted`, hoặc peer loopback khi `!trustLoopback`: gỡ chính nó, `fireChannelActive()`.
    - Ngược lại: thêm `HAProxyMessageDecoder` ngay sau nó, hẹn đóng sau `headerTimeout`, **không** forward `channelActive`.
  - `channelRead(HAProxyMessage)`:
    - `PROXY` với TCP4/TCP6: đặt `CLIENT_ADDRESS`. `LOCAL`: không đặt.
    - Sau đó `release`, gỡ decoder và chính nó, hủy timer, `fireChannelActive()`.
    - Byte đầu tiên không phải header PROXY (`HAProxyProtocolException`): tăng `gw.admission.rejected{reason=proxy}` rồi đóng.
- `AcceptGate.remote()` dùng `ChannelAttrs.clientAddress`. Kiểm tra loopback của warm-up trong `HelloAuthHandler` dùng `ChannelAttrs.socketPeer(ctx.channel())`.
- Initializer: chỉ khi `enabled`, `p.addFirst("proxy", new ProxyProtocolHandler(...))`. `trusted-cidrs` rỗng mà `enabled=true` → fail-fast khi khởi động.

- [ ] **Step 1: Viết test thất bại**

```java
// CidrSetTest
@Test void matchesV4AndV6Prefixes() { /* 10.1.2.3 ∈ 10.0.0.0/8; 11.0.0.1 ∉; 2001:db8::1 ∈ 2001:db8::/32; 192.0.2.7 ∈ "192.0.2.7" */ }
@Test void rejectsMalformedEntry() { /* "10.0.0.0/33" → IllegalArgumentException chứa "10.0.0.0/33" */ }
// ProxyProtocolHandlerTest — EmbeddedChannel (socketPeer = loopback); trusted = CidrSet.parse(["127.0.0.0/8"]), trustLoopback = true;
// riêng test untrusted dùng trusted = ["10.0.0.0/8"]
@Test void trustedPeerHeaderSetsClientAddressBeforeAcceptGate() {
  /* writeInbound "PROXY TCP4 203.0.113.7 10.0.0.1 5555 8443\r\n" → handler cuối ghi nhận channelActive đúng 1 lần, sau header;
     clientAddress == 203.0.113.7; pipeline không còn "proxy" lẫn decoder */ }
@Test void channelActiveIsHeldUntilTheHeader() { /* chưa có byte → handler cuối chưa thấy channelActive */ }
@Test void localCommandKeepsSocketAddress() { /* header v2 LOCAL → clientAddress == socketPeer */ }
@Test void missingHeaderTimesOut() { /* advanceTimeBy(5 s) → channel đóng */ }
@Test void garbageInsteadOfHeaderIsRejected() { /* 0x16 0x03 … (TLS ClientHello) → đóng, gw.admission.rejected{reason=proxy} == 1 */ }
@Test void untrustedPeerHeaderIsNotParsed() { /* peer ngoài trusted → channelActive forward ngay; byte "PROXY …" đi tiếp nguyên vẹn
     tới handler sau (ByteBuf), clientAddress == socketPeer */ }
// WarmUpHelloTest
@Test void proxyClaimedLoopbackDoesNotUnlockWarmUp() {
  /* socket peer 10.0.0.9 (channel attr seam), CLIENT_ADDRESS=127.0.0.1 → token warm-up → 4001 */ }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service test -Dtest='CidrSetTest,ProxyProtocolHandlerTest,WarmUpHelloTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực theo Interfaces**

`WarmUpHelloTest.connect` hiện truyền `InetAddress` vào `tracker.admit`. Thêm seam để đặt socket peer giả: `ChannelAttrs.SOCKET_PEER_OVERRIDE`, chỉ đọc khi channel không phải `InetSocketAddress`.

- [ ] **Step 4: Chạy toàn bộ unit**

Run: `./mvnw -q -f gateway-service test`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): PROXY protocol v1/v2 from trusted CIDRs"
```

---

### Task 4: Tenant cap — quota local và mượn qua Redis

**Files:**
- Create: `tenant/TenantConnCounter.java`, `tenant/TenantCapBorrower.java`, `tenant/CellSize.java`
- Modify: `session/Session.java` (+`tenantReserved`), `session/SessionTracker.java`, `pipeline/HelloAuthHandler.java`, `pipeline/PipelineDeps.java`, `config/EdgeConfig.java`, `config/GatewayConfig.java`
- Test: `tenant/TenantConnCounterTest.java`, `pipeline/HelloAuthHandlerTest.java` (bổ sung), `src/it/.../tenant/TenantCapBorrowerIT.java`

**Interfaces:**
- Produces:
  ```java
  public final class TenantConnCounter {                 // thread-safe, ConcurrentHashMap<String, AtomicInteger>
    public boolean tryReserve(String tenant, int quota); // CAS +1 nếu < quota
    public void reserveBorrowed(String tenant);          // +1 không kiểm (đã mượn được)
    public void release(String tenant);                  // −1, gỡ entry về 0
    public int count(String tenant);
    public Map<String, Integer> snapshot();
  }
  public final class CellSize {                          // nodesInCell, volatile int ≥ 1
    public int nodes();
    public void refresh();                               // offload: ZCOUNT nodes:live (now-30s) +inf trên Redis registry, timeout 1 s
  }
  public final class TenantCapBorrower {
    public TenantCapBorrower(RedisConnections redis, String nodeId, LongSupplier wallMillis, GatewayMetrics m);
    public CompletionStage<Boolean> borrow(String tenant, int maxConns, int ownTotalAfter); // timeout 200 ms; lỗi → false + result=error
    public void report(Map<String, Integer> totals, java.util.function.ToIntFunction<String> maxConns); // 10 s, chỉ tenant có cap
    public static int quota(int maxConns, int nodes);    // ceil(maxConns × 1.1 / nodes)
  }
  // Session: boolean tenantReserved(); void tenantReserved(boolean)
  ```
  - Key `tenantconns:{<tenant>}` là hash. Field là nodeId, value là `"<total>|<expiresAtMs>"`, hạn 30 s.
  - Lua `BORROW(KEYS[1]; ARGV node, ownTotalAfter, max, nowMs, ttlMs)`: cộng các field khác node này còn hạn, cộng `ownTotalAfter`.
    - Tổng > `max` → trả 0.
    - Ngược lại → `HSET` field của node với `ownTotalAfter|now+ttl`, `PEXPIRE` key `2×ttl`, trả 1.
  - `report` dùng `HSET` (total > 0) hoặc `HDEL` (total = 0), không kiểm tra.
- `HelloAuthHandler`: bước i chạy sau `admitted()` và trước `activate()`, chỉ khi `policy.maxConns() > 0` và không phải warm-up.
  - `counter.tryReserve(t, quota)` thành công → activate.
  - Thất bại → `autoRead=false`, gọi `borrower.borrow(t, max, counter.count(t) + 1)`. Kết quả quay về loop; session phải vẫn HANDSHAKING.
    - `true` → `reserveBorrowed`, activate.
    - `false` → `gw.handshakes{result=tenant_cap}`, close `OVERLOADED` với retryAfter ∈ [5000, 30000].
  - Sau khi reserve: `session.tenantReserved(true)`. Đường close nào trước khi activation thành công cũng `release`.
- `SessionTracker.onClosed`: `tenantReserved()` thì `counter.release(tenant)`.
- `EdgeConfig`: mỗi 10 s trên offload chạy `cellSize.refresh()` và `borrower.report(counter.snapshot(), t -> tenants.policy(t).maxConns())`.

- [ ] **Step 1: Viết test thất bại**

```java
// TenantConnCounterTest
@Test void reserveStopsAtQuotaAndReleaseFreesIt() { }
@Test void concurrentReservesNeverExceedQuota() { /* 8 thread × 1000 tryReserve(quota=100) → đúng 100 true */ }
@Test void quotaRoundsUpWithTenPercentHeadroom() {
  assertThat(TenantCapBorrower.quota(1000, 3)).isEqualTo(367); assertThat(TenantCapBorrower.quota(2, 1)).isEqualTo(3); }
// HelloAuthHandlerTest — tenant t1 maxConns=2 (TenantLimitsStore), nodes=1 → quota 3; borrower giả (CompletableFuture điều khiển được)
@Test void underLocalQuotaConnectsWithoutRedis() { /* borrower.calls == 0 */ }
@Test void overQuotaBorrowsAndConnects() { /* counter đã 3; borrower → true → CONNECTED; count == 4; tenantReserved */ }
@Test void deniedBorrowClosesOverloadedWithRetryAfter() {
  /* borrower → false → ERROR 4013, retry_after_ms ∈ [5000,30000]; counter.count == 3; gw.handshakes{result=tenant_cap} == 1 */ }
@Test void borrowErrorIsTreatedAsDenied() { /* failedFuture → 4013 */ }
@Test void closeReleasesTheReservation() { /* CONNECTED rồi channel.close → count giảm 1 */ }
@Test void sessionClosedWhileBorrowingDoesNotActivate() { /* đóng channel trước khi future hoàn tất → không CONNECTED, count không đổi */ }
// TenantCapBorrowerIT — Redis Testcontainers
@Test void borrowSumsOtherNodesAndRejectsAboveMax() { /* n2 report 5; n1 borrow(max 6, ownAfter 1) true; borrow(max 6, ownAfter 2) false */ }
@Test void expiredNodeEntriesAreIgnored() { /* HSET n2 "5|<now-1>" → n1 borrow(max 6, ownAfter 6) true */ }
@Test void reportOfZeroRemovesTheField() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service test -Dtest='TenantConnCounterTest,HelloAuthHandlerTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực theo Interfaces**

Lua đặt cạnh các script registry (`LuaScript`). Redis standalone ở local và dev chạy cùng script. Hash-tag `{tenant}` giữ cả key trong một slot.

- [ ] **Step 4: Chạy unit và IT mới**

Run: `./mvnw -q -f gateway-service test` rồi `./mvnw -q -f gateway-service verify -Dit.test=TenantCapBorrowerIT -Dtest=NoSuch -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS.

- [ ] **Step 5: Đo heap**

Run: `make memory-check`. Expected: < 5120 B/session (thêm hai boolean). Ghi số đo vào ledger.

- [ ] **Step 6: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): tenant connection cap with local quota and Redis borrowing"
```

---

### Task 5: Định tuyến HTTP và `POST /v1/upstream`

**Files:**
- Create: `pipeline/HttpUpstreamHandler.java`
- Modify: `pipeline/UpgradeGuard.java`, `pipeline/WsChannelInitializer.java` (aggregator `max(8192, frameMax + 1024)`), `upstream/UpstreamPublisher.java`, `auth/HelloPolicy.java`, `pipeline/PipelineDeps.java`
- Test: `pipeline/UpgradeGuardTest.java` (bổ sung), `pipeline/HttpUpstreamHandlerTest.java`, `upstream/UpstreamPublisherTest.java` (bổ sung)

**Interfaces:**
- Produces:
  ```java
  // HelloPolicy: kiểm tra c, d, e của §4.6 cho một ticket không qua HELLO (không có sdk_version, Origin)
  public Optional<Rejection> checkTicketUse(ResumeTicket t);
  // UpstreamPublisher
  public enum HttpOutcome { ACCEPTED, UNKNOWN_TYPE, MISSING_MSG_ID, UNAVAILABLE }
  public CompletionStage<HttpOutcome> publishHttp(ResumeTicket t, Frame f);
  //   cùng luật type/telemetry/shed với publish(); conn_id rỗng; telemetry bị drop vẫn là ACCEPTED (đếm dropped);
  //   cmd|cmd_result hoàn tất khi Kafka callback; ngoại lệ send → UNAVAILABLE
  public final class HttpUpstreamHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    public HttpUpstreamHandler(ResumeTicketService tickets, HelloPolicy policy, UpstreamPublisher upstream,
        LimitsHolder limits, GatewayMetrics m);
  }
  ```
- `UpgradeGuard.channelRead`. Kiểm tra 404 và 503 chạy trước, theo thứ tự hiện có. Sau đó:
  - `/v1/ws`: giữ nguyên (subprotocol, upgrade).
  - `/v1/upstream`: thay `ws`, `wsBinary`, `frameCodec`, `hello`, `session` bằng `"httpUpstream"`; gỡ chính nó; forward request.
  - `/v1/sse`: xử lý ở Task 6. Tới Task 6 thì trả 404.
- `HttpUpstreamHandler`: kiểm theo thứ tự method → content-type → độ dài → ticket → `checkTicketUse` → parse `Frame` (type phải là UPSTREAM) → `publishHttp`. Mã HTTP theo Global Constraints.
  - Response luôn có `Connection: close`, ghi trên event loop của channel, rồi đóng.
  - Tăng `gw.http.upstream{status}`.
  - Request đã qua `AcceptGate`, nên session HANDSHAKING của nó vẫn bị deadline HELLO 5 s giới hạn.

- [ ] **Step 1: Viết test thất bại**

```java
// UpgradeGuardTest
@Test void upstreamPathNeedsNoSubprotocolAndRewiresThePipeline() { /* POST /v1/upstream → pipeline có "httpUpstream", không còn "ws" */ }
@Test void upstreamPathStill503WhenShedding() { }
// HttpUpstreamHandlerTest — EmbeddedChannel; UpstreamPublisher với KafkaClients giả như UpstreamPublisherTest
@Test void validCmdIs202AfterKafkaAck() { /* response chưa có trước khi callback; sau callback → 202, Connection: close, channel đóng */ }
@Test void telemetryIs202AtOnce() { }
@Test void kafkaFailureIs503() { }
@Test void expiredOrForgedTicketIs401() { }
@Test void revokedOrWrongCellIs403() { }
@Test void blockedTenantIs429WithRetryAfter300() { }
@Test void unknownTypeOrMissingMsgIdIs400() { }
@Test void nonUpstreamFrameIs400() { /* Frame(PING) */ }
@Test void getIs405AndWrongContentTypeIs415AndOversizeIs413() { }
// UpstreamPublisherTest
@Test void httpRecordHasTicketIdentityAndEmptyConnId() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service test -Dtest='UpgradeGuardTest,HttpUpstreamHandlerTest,UpstreamPublisherTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực theo Interfaces**

Phần dựng `UpstreamRecord` và `ProducerRecord` tách thành một method private dùng chung cho `publish` và `publishHttp`. Không nhân đôi luật type/telemetry.

- [ ] **Step 4: Chạy toàn bộ unit**

Run: `./mvnw -q -f gateway-service test`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): HTTP upstream endpoint POST /v1/upstream"
```

---

### Task 6: SSE fallback `POST /v1/sse`

**Files:**
- Create: `pipeline/SseAdapter.java`
- Modify: `pipeline/UpgradeGuard.java`, `protocol/ChannelAttrs.java` (`TransportKind.SSE`), `protocol/ChannelCloser.java`, `pipeline/HelloAuthHandler.java` (từ chối khi tenant tắt SSE), `downstream/PushDelivery.java`, `metrics/GatewayMetrics.java` (`gw.sse.sessions`)
- Test: `pipeline/SseAdapterTest.java`, `protocol/ChannelCloserTest.java` (bổ sung), `downstream/PushDeliveryTest.java` (bổ sung)

**Interfaces:**
- Produces:
  ```java
  public final class SseAdapter extends ChannelDuplexHandler {
    public SseAdapter(Duration heartbeat);                // 30 s
    static HttpResponseStatus httpStatus(CloseCode c);    // dùng khi chưa gửi header 200
  }
  ```
  - **Inbound:** nhận `FullHttpRequest` (POST, `application/x-protobuf`, body ≤ `hello-max`; sai thì 405, 415 hoặc 413). Đặt `TRANSPORT=SSE` và `PROTOCOL_READY`, lưu Origin, rồi phát `ByteBuf` của `Frame{type=HELLO, payload=body}` cho `FrameCodec`. Mọi inbound sau đó bị bỏ.
  - **Outbound:** mỗi `ByteBuf` (frame đã encode) thành `DefaultHttpContent("data: " + base64 + "\n\n")`. Lần ghi đầu tiên gửi trước `HttpResponse 200` với các header:
    - `content-type: text/event-stream`
    - `cache-control: no-cache`
    - `transfer-encoding: chunked`
  - **Heartbeat:** sau khi đã gửi header, `ctx.executor().scheduleAtFixedRate` mỗi `heartbeat` ghi `":\n\n"`; ghi thành công thì `session.touch(loop.nowSec())`. Timer bị hủy khi channel inactive.
  - `httpStatus`: 4001/4002 → 401, 4003 → 403, 4009 → 429, 4011 → 421, 4013 → 503, còn lại → 400.
- `UpgradeGuard`, path `/v1/sse`: gỡ `aggregator`; thay `ws` và `wsBinary` bằng `"sse"` (đặt trước `frameCodec`); gỡ chính nó; forward request.
- `ChannelCloser`, transport SSE:
  - Chưa gửi header: response `httpStatus(code)` kèm `Retry-After` (giây) nếu có, rồi đóng.
  - Đã đang stream: event `data:` chứa `Frame(ERROR, fatal=true)`, rồi `LastHttpContent`, rồi đóng.
- `HelloAuthHandler`: transport SSE mà `!policy.sseFallbackEnabled()` → `AUTH_FAILED`, `gw.handshakes{result=sse_disabled}`.
- `PushDelivery`: session SSE xử lý RELIABLE/ORDERED như `downgradeReliable`, tức là ghi xong trả `STATUS_WRITTEN` (xem Ruling R1).

- [ ] **Step 1: Viết test thất bại**

```java
// SseAdapterTest — pipeline: SseAdapter → FrameCodec → HelloAuthHandler → SessionHandler (như HelloAuthHandlerTest), tenant bật SSE
@Test void helloInBodyStreamsConnectedAsFirstEvent() {
  /* POST Hello(jwt) → outbound: HttpResponse 200 text/event-stream; HttpContent "data: <b64>\n\n" → Frame CONNECTED có resume_ticket */ }
@Test void tenantWithoutSseGets401() { /* response 401, channel đóng; gw.handshakes{result=sse_disabled} == 1 */ }
@Test void wrongMethodOrContentTypeIsRejected() { /* GET → 405; text/plain → 415 */ }
@Test void heartbeatCommentTouchesSession() { /* advanceTimeBy(30 s) → HttpContent ":\n\n"; session lastSeen == now */ }
@Test void closeWhileStreamingSendsErrorEventAndLastChunk() { /* session.close(REVOKED) → data: Frame ERROR(4003,fatal) rồi LastHttpContent */ }
@Test void authFailureBeforeStreamingIsHttp401() { }
// PushDeliveryTest
@Test void reliablePushOnSseIsAckedWritten() { /* TRANSPORT=SSE → STATUS_WRITTEN ngay, pending window rỗng */ }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service test -Dtest='SseAdapterTest,ChannelCloserTest,PushDeliveryTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực theo Interfaces**

`WritabilityTracker` vẫn nằm sau `flush`, nên slow consumer và budget áp dụng cho SSE mà không cần sửa gì.

- [ ] **Step 4: Chạy toàn bộ unit**

Run: `./mvnw -q -f gateway-service test`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): SSE fallback POST /v1/sse"
```

---

### Task 7: Kill switch `trace.principal`

**Files:**
- Modify: `platform-contracts/proto/pangate/v1/control.proto`, `control/ControlKeys.java`, `control/ControlEventHandler.java`, `control/ControlEnforcer.java`, `session/Session.java` (+`traced`), `pipeline/HelloAuthHandler.java`, `protocol/FrameCodec.java`, `config/EdgeConfig.java`, `dev/DevSecretsGenerator.java` (đã sinh `principal-hash.key`)
- Create: `trace/TraceTargets.java`, `trace/PrincipalHasher.java`, `trace/SessionTrace.java`
- Test: `trace/TraceTargetsTest.java`, `trace/PrincipalHasherTest.java`, `control/ControlEventConsumerTest.java` (bổ sung), `protocol/FrameCodecTest.java` (bổ sung)

**Interfaces:**
- Produces:
  ```proto
  // control.proto — key "trace:<principal>"; value rỗng = tombstone
  message TracePrincipal { string principal_id = 1; int64 expires_at_ms = 2; }
  // ControlEvent.oneof event: TracePrincipal trace = 5;
  ```
  ```java
  public final class TraceTargets {                          // thread-safe
    public static final Duration MAX_TTL = Duration.ofMinutes(15);
    public void put(String principal, long issuedAtMs, long expiresAtMs);   // hạn = min(expires>0 ? expires : ∞, issued + 15 m)
    public void remove(String principal);
    public boolean traced(String principal, long nowMs);
    public List<String> sweep(long nowMs);                    // gỡ và trả các principal đã hết hạn
  }
  public final class PrincipalHasher { public static PrincipalHasher load(SecretSource s); public String hash(String principal); } // 16 hex
  public final class SessionTrace {                           // logger "pangate.trace", INFO
    public static void activated(Session s, PrincipalHasher h);
    public static void frame(Session s, String dir, FrameType t, String msgId, int bytes);  // dir = "in" | "out"
    public static void closed(Session s, CloseCode c, String reason);
  }
  // Session: boolean traced(); void traced(boolean)
  ```
- Bật và tắt cờ `traced`:
  - `HelloAuthHandler.activate` đặt `traced` theo `TraceTargets.traced(principal, now)`.
  - Khi `put` hoặc `remove`, `ControlEnforcer` dùng `PrincipalIndex` để đặt hoặc xóa cờ trên owner loop của từng session.
  - `EdgeConfig` mỗi 10 s gọi `sweep`, rồi xóa cờ của các principal hết hạn theo cùng cách.
- `FrameCodec`: decode và encode gọi `SessionTrace.frame` chỉ khi `session.traced()`. `PreEncodedFrame` được ghi là `PUSH` kèm số byte.
- `SessionTracker.onClosed`: nếu `traced` thì gọi `SessionTrace.closed`.

- [ ] **Step 1: Viết test thất bại**

```java
// TraceTargetsTest
@Test void ttlIsCappedAt15Minutes() { /* put(p, t0, t0 + 1h) → traced(t0+14m) true, traced(t0+15m+1) false */ }
@Test void missingExpiryMeans15Minutes() { }
@Test void tombstoneRemoves() { }
@Test void sweepReturnsExpired() { }
// PrincipalHasherTest
@Test void hashIsStable16HexAndKeyDependent() { }
// ControlEventConsumerTest
@Test void traceKeyAppliesAndTombstoneRemoves() { /* "trace:t1:u1" → TraceTargets.traced; value rỗng → false */ }
@Test void tracedFlagFollowsLiveSessions() { /* session ACTIVE của t1:u1 → sau event traced()==true; tombstone → false */ }
// FrameCodecTest — ListAppender trên logger "pangate.trace"
@Test void tracedSessionLogsFramesWithHashedPrincipalOnly() {
  /* PING in, PONG out → 2 dòng chứa "in PING" / "out PONG"; không dòng nào chứa "t1:u1" */ }
@Test void untracedSessionLogsNothing() { }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f platform-contracts install && ./mvnw -q -f gateway-service test -Dtest='TraceTargetsTest,PrincipalHasherTest,ControlEventConsumerTest,FrameCodecTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực theo Interfaces**

Logger `pangate.trace` có level riêng (INFO) trong `logback-spring.xml` hoặc `application.yml` (`logging.level.pangate.trace: INFO`), để vẫn ghi khi root ở WARN.

- [ ] **Step 4: Chạy toàn bộ unit**

Run: `./mvnw -q -f gateway-service test`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add platform-contracts gateway-service
git commit -m "feat(gateway): trace.principal kill switch with hashed per-session trace log"
```

---

### Task 8: Span OTel `gw.push.handoff` và `gw.push.write`

**Files:**
- Modify: `gateway-service/pom.xml`, `config/GatewayProperties.java` (`Tracing(String otlpEndpoint)`), `application.yml` (`tracing.otlp-endpoint: ""`), `downstream/PushDelivery.java`, `config/EdgeConfig.java`
- Create: `trace/GatewayTracing.java`
- Test: `trace/GatewayTracingTest.java`, `downstream/PushDeliveryTest.java` (bổ sung)

**Interfaces:**
- Consumes: `Session.traced()` (Task 7).
- Produces:
  ```java
  public final class GatewayTracing {
    public static GatewayTracing noop();
    public static GatewayTracing create(OpenTelemetry otel);       // test: SdkTracerProvider + SimpleSpanProcessor(InMemorySpanExporter)
    public static boolean sampled(String traceparent);             // W3C "00-<32hex>-<16hex>-<2hex>", flags & 1; sai định dạng → false
    public Span handoff(Frame f, Session s, long enqueuedAtMs);   // start = enqueuedAtMs; parent = traceparent nếu hợp lệ
    public Span write(Span handoff);                               // con của handoff
    public void nack(Frame f, AckItem.Status st, long enqueuedAtMs); // span handoff đơn, status ERROR, attr pangate.nack
  }
  ```
  - Attribute: `pangate.msg_id`, `pangate.delivery_class`, `pangate.tenant`, `pangate.principal_hash`. Không có principal thô hay payload.
- `PushDelivery.write`: khi `sampled(traceparent) || s.traced()`:
  - `handoff` kết thúc ngay lúc gọi write.
  - `write` kết thúc khi `ch.write(frame)` hoàn tất. Đường này dùng promise thật thay cho `voidPromise`.
  - Promise lỗi → status ERROR.
- Các đường NACK của `deliver` (`NOT_FOUND`, `OVERFLOW`) gọi `tracing.nack(...)` bất kể sampled.
- `EdgeConfig`: `otlp-endpoint` rỗng → `GatewayTracing.noop()`. Ngược lại dựng SDK: `BatchSpanProcessor` với `OtlpHttpSpanExporter` (sender JDK), resource `service.name=gateway`, `service.instance.id=<nodeId>`; đóng khi shutdown.

- [ ] **Step 1: Viết test thất bại**

```java
// GatewayTracingTest
@Test void sampledFlagParsing() {
  assertThat(GatewayTracing.sampled("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")).isTrue();
  assertThat(GatewayTracing.sampled("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")).isFalse();
  assertThat(GatewayTracing.sampled("garbage")).isFalse(); assertThat(GatewayTracing.sampled(null)).isFalse(); }
// PushDeliveryTest — PushDelivery với GatewayTracing.create(in-memory)
@Test void sampledPushEmitsHandoffAndWriteUnderTheCallersTrace() {
  /* 2 span: gw.push.handoff, gw.push.write; traceId == 4bf92f…; write.parent == handoff; không attr nào chứa "t1:" */ }
@Test void unsampledPushEmitsNothing() { }
@Test void tracedSessionIsAlwaysTraced() { /* không traceparent, s.traced(true) → 2 span */ }
@Test void nackNotFoundEmitsErrorSpanEvenUnsampled() { /* 1 span handoff, status ERROR, pangate.nack=STATUS_NACK_NOT_FOUND */ }
```

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service test -Dtest='GatewayTracingTest,PushDeliveryTest'`
Expected: lỗi compile.

- [ ] **Step 3: Hiện thực theo Interfaces**
- Nếu `banDuplicateClasses` của Enforcer báo trùng class sau khi thêm OTel, loại phần trùng trong `pom.xml`, không tắt luật.
- BlockHound có thể bắt `BatchSpanProcessor` park khi tranh lock queue. Nếu xảy ra: whitelist `BatchSpanProcessor$Worker.addSpan` trong `BlockHoundExtension`, kèm ledger Ruling ghi bằng chứng (stack trace).

- [ ] **Step 4: Chạy toàn bộ unit**

Run: `./mvnw -q -f gateway-service test`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway-service
git commit -m "feat(gateway): OTel spans for push handoff and write"
```

---

### Task 9: IT M3b, R1–R3, tài liệu

**Files:**
- Create: `gateway-service/src/it/java/com/pangate/gateway/it/GatewayEdgeIT.java`
- Modify: `gateway-devtools/.../client/GatewayClient.java` (+`proxyHeader(InetSocketAddress src)`: thêm `HAProxyMessageEncoder` trước TLS và gửi header v1 khi `channelActive`), `GatewayClientTest`, spec (§4.3, §4.6 i, §5.10, §7.7, §7.8, §8, §9, §14), `gateway-service/README.md`, plan này (`## Kết quả đo M3b`)
- Test: `GatewayEdgeIT` (6 kịch bản), `GatewayClientTest` (bổ sung)

**Interfaces:**
- Consumes: mọi Produces ở Task 1–8.
- IT dùng `Gw.start(...)` như `GatewayProtectionIT` (profile local, infra Testcontainers, `warmup.iterations=0`). Client SSE và HTTP dùng `java.net.http.HttpClient` tin CA dev.

- [ ] **Step 1: Viết test thất bại**

```java
// GatewayClientTest
@Test void proxyHeaderIsSentBeforeTls() { /* server Netty với HAProxyMessageDecoder ghi nhận src 203.0.113.7:5555 */ }
// GatewayEdgeIT
@Test void certificateReplacedOnDiskIsServedWithoutRestart() {
  /* --gateway.tls.reload-interval=1s; serial cert lúc đầu; DevSecretsGenerator.reissueGatewayCert(devDir); await ≤ 5 s:
     kết nối mới thấy serial mới; client cũ vẫn PING được */ }
@Test void sseStreamsConnectedAndPushes() {
  /* control tenantcfg tenant t1 sse_fallback_enabled=true; POST /v1/sse Hello → đọc dòng "data:" → CONNECTED;
     FakeRouter unicast RELIABLE tới conn_id → event PUSH; router nhận STATUS_WRITTEN */ }
@Test void httpUpstreamWithResumeTicketReachesKafka() {
  /* ticket từ CONNECTED của một client WS; POST /v1/upstream Frame(UPSTREAM type=cmd) → 202; consumer đọc được record c1.gw.upstream.cmd
     với principal đúng; ticket sửa 1 byte → 401 */ }
@Test void proxyProtocolFeedsPerIpLimit() {
  /* --gateway.listeners.proxy-protocol.enabled=true --…trusted-cidrs=127.0.0.0/8 --…trust-loopback=true --gateway.limits.per-ip=1;
     client proxyHeader(203.0.113.7) CONNECTED; client thứ hai cùng src bị đóng ở AcceptGate; client src 203.0.113.8 → CONNECTED */ }
@Test void tenantCapRejectsAboveMaxConnsWith4013() {
  /* tenantcfg t2 max_conns=2 → quota 3 (1 node); client 1–3 CONNECTED; client 4 → ERROR 4013, retry 5–30 s;
     đóng client 1 → client 5 CONNECTED */ }
@Test void tracePrincipalLogsOnlyThatPrincipal() {
  /* ControlEvent trace:t1:traced → ListAppender "pangate.trace": client t1:traced PING → có "in PING"; client t1:other → không dòng nào */ }
```

> **Seam test cho PROXY:** loopback mặc định được coi là kết nối trực tiếp (Global Constraints), nên IT bật `trust-loopback` (Task 3) để client loopback vẫn phải gửi header. Đây là cách duy nhất chạy PROXY end-to-end trong một máy.

- [ ] **Step 2: Chạy để thấy thất bại**

Run: `./mvnw -q -f gateway-service verify -Dit.test=GatewayEdgeIT -Dtest=NoSuch -Dsurefire.failIfNoSpecifiedTests=false`
Expected: lỗi compile (thiếu `GatewayClient.proxyHeader`).

- [ ] **Step 3: Hiện thực phần còn thiếu**

Gồm `GatewayClient.proxyHeader` và các sửa mà IT lộ ra; mỗi sửa ngoài plan cần một Ruling trong ledger.

- [ ] **Step 4: Toàn bộ suite và R1–R3**

Run:
- `make bootstrap && ./mvnw -f gateway-service verify` → Expected: unit và IT xanh.
- `make local-check` → `LOCAL START OK`.
- `make up && make smoke && docker compose -f platform-devenv/docker-compose.yml stop gateway` → healthy, smoke OK, stop ≤ 15 s.
- `make memory-check` → < 5120 B.
- `make load-check` → 18k, lag p99 < 20 ms, shed level 0.

- [ ] **Step 5: Tài liệu**
- **Spec:**
  - §4.3: mã HTTP, `Connection: close`, ack-on-write của SSE (Ruling R1).
  - §4.6 i và §5.10 bước 4: hash `tenantconns` có hạn thay cho `INCR`/`DECR` (Ruling R2).
  - §7.7: định dạng `tls-ticket-keys`.
  - §7.8: key `trace:<principal>`.
  - §8: attribute span.
  - §9: `tls.reload-interval`, `tracing.otlp-endpoint`.
  - §14: thêm 2 khác biệt có chủ đích.
- **README:** layout secret mới (`tls-ticket-keys/`); cách bật trace một principal (`devtools` hoặc ghi thẳng `gw.control`).
- **Plan:** điền `## Kết quả đo M3b`.

- [ ] **Step 6: Commit**

```bash
git add gateway-service gateway-devtools docs
git commit -m "test(gateway): M3b edge IT, measurements and docs"
```

---

## Rulings của plan (đã được xác nhận ngày 2026-11-04)

- **R1 — RELIABLE trên SSE được ack ngay khi ghi (`STATUS_WRITTEN`).**
  - Spec §4.3 nói "ngữ nghĩa downstream giống hệt WS", nhưng client SSE không có kênh để gửi ACK: `/v1/upstream` chỉ nhận `UPSTREAM`, và POST có thể rơi vào node khác với node giữ session.
  - Nếu giữ pending window như WS, mọi push RELIABLE sẽ timeout (`NACK_TIMEOUT`) và làm đầy window sau 32 message.
  - Hậu quả nếu sai: với client SSE, router coi push là đã giao khi gateway mới chỉ ghi xong, nên có thể mất message khi client rớt mạng ngay sau đó. Client phục hồi qua `seq` và replay của Push API.
- **R2 — `tenantconns:{t}` là hash `node → "total|expiresAtMs"` với hạn 30 s, thay cho `INCR`/`DECR` của spec.**
  - Với `INCR`/`DECR`, một node bị SIGKILL để lại số đếm thừa vĩnh viễn.
  - Hash có hạn tự lành sau 30 s và so được tổng thật của cả cell với `maxConns`.
  - Hậu quả nếu sai: thêm một `HSET` cho mỗi tenant có cap, mỗi node, mỗi 10 s; trong cửa sổ 10 s, tổng có thể vượt cap tối đa bằng số kết nối mới nhận trong quota local.
- **R3 — `/v1/upstream` chỉ một request mỗi kết nối (`Connection: close`).** Mỗi POST đi qua `AcceptGate` (per-IP, admission bucket), nên session HANDSHAKING của nó chỉ sống tối đa 5 s. Hậu quả nếu sai: thiết bị gửi nhiều upstream qua HTTP tốn thêm một handshake TLS mỗi lần, và chia admission bucket với các kết nối WS.
- **R4 — Thiếu `tls-ticket-keys` thì fail-fast**, giống `ticket-keys` từ M1. Profile local và dev tự sinh. Hậu quả nếu sai: môi trường prod chưa cấp secret này sẽ không khởi động được.

## Kết quả đo M3b

| Kiểm tra | Kết quả | Ngưỡng |
|---|---|---|
| Test gateway | 503 unit + 96 IT xanh (gồm `GatewayEdgeIT` 6 kịch bản, `TenantCapBorrowerIT` 4) | — |
| `make local-check` | LOCAL START OK; SIGTERM khi có client ngừng đọc: 5,4 s | ≤ 15 s |
| Compose dev | healthy sau 23 s; `make smoke` OK (push → ack 231 ms); `docker compose stop gateway` 0,7 s | ≤ 90 s / ≤ 15 s |
| Heap/session | 4794 B (thêm `tenantReserved`, `traced`) | < 5120 B |
| `make load-check` 18k | lag p99 0,68 ms; shed level 0 ở trạng thái ổn định (L1 lúc ramp, về 0 sau ~97 s) | < 20 ms, level 0 |
| Dependency mới | `netty-codec-haproxy` 4.2.20; OTel 1.49.0 (sender JDK, không okhttp) | — |
