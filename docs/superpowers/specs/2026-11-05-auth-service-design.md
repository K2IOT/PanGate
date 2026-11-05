# auth-service — Design Spec

- **Ngày:** 2026-11-05
- **Trạng thái:** Draft, chờ duyệt
- **Nguồn kiến trúc:** *Netty Gateway — 10M Active Connections* (Nov 2, 2026, @Kuu Kuu), gọi tắt là "tài liệu gốc"; mục số trong ngoặc, ví dụ (§3.6), là mục của tài liệu gốc. "Spec gateway" là `docs/superpowers/specs/2026-11-03-gateway-service-design.md`.
- **Vị trí trong lộ trình:** sub-project 1/4 sau bản `0.1.0` của gateway: (1) `auth-service`, (2) contracts cho đường push, (3) `push-router-service`, (4) `push-api-service`. Mỗi sub-project có spec, plan và hiện thực riêng.

---

## 0. Mục tiêu, tiêu chí thành công, phạm vi

**Mục tiêu.** Hiện thực Auth & Token service của Phase 1 (§3.2) cho tenant backend:
- Cấp access token OAuth2 client credentials (15 phút) cho backend của tenant, theo scope (§3.6).
- Mint connect JWT ES256 (10 phút) cho người dùng của tenant qua `POST /v1/tokens`, đúng định dạng mà `JwtVerifier` của gateway đang kiểm tra.
- Phát JWKS và tự xoay khoá ký: key sống 30 ngày, chồng lấn 7 ngày (§3.2).
- Thu hồi session ngay lập tức bằng `REVOKE` trên `gw.control` (§3.3).
- Quản lý tenant và client qua Admin API nội bộ.

**Tiêu chí thành công của spec.** Kỹ sư hoặc coding agent hiện thực được mà không phải đoán về endpoint, claim, bảng, job nền, hành vi lỗi hay cách test.

**Quyết định đã chốt khi brainstorm (2026-11-05):**

| # | Câu hỏi | Quyết định |
|---|---|---|
| D1 | Bố cục repo | Monorepo, thư mục `auth-service/` build độc lập (ADR 0008) |
| D2 | Phạm vi người dùng cuối | Chỉ phục vụ tenant backend. Không có refresh token, không có token exchange trong v1 |
| D3 | Tạo tenant và client | Admin API nội bộ trên cổng quản trị, scope `admin`; seed cho local/dev |
| D4 | Lưu và xoay khoá ký | Auth tự sinh key ES256, private key mã hoá AES-GCM bằng KEK từ `SecretSource`, lưu PostgreSQL; publish trước khi ký; advisory lock |
| D5 | Dạng token API | JWT ES256 cùng bộ key, `aud=pangate-api`; Push API kiểm tra offline qua JWKS |
| D6 | Phát sự kiện thu hồi | Transactional outbox trong PostgreSQL → relay → `gw.control` |
| D7 | Cách hiện thực | Spring Boot MVC + virtual threads + `JdbcClient` + Flyway, kiến trúc hexagonal (§13.5); ký bằng Nimbus JOSE |

**Ngoài phạm vi:** refresh token cho SDK, token exchange RFC 8693, mTLS/SPIFFE giữa các service, Vault thật trong devenv, giao diện Admin Console (Phase 3), service Audit riêng (Phase 3), metering.

**Nguyên tắc bất biến** (từ tài liệu gốc):
1. Reconnect không bao giờ phụ thuộc Auth (§2, §8): gateway dùng resume ticket và JWKS đã cache. Auth sập chỉ ảnh hưởng đăng nhập mới.
2. JWKS phải tiếp tục được phục vụ khi PostgreSQL sập.
3. Không lệnh thu hồi nào bị mất: ghi DB trước, đẩy Kafka sau.
4. Mỗi service sở hữu store của mình (§13.7): không service nào khác đọc hay ghi schema `auth`.

---

## 1. Kiến trúc và bố cục

`auth-service/` ở gốc repo; parent `platform-parent`; phụ thuộc `platform-contracts` (Protobuf `ControlEvent`) và `platform-spring-starter` (observability, `SecretSource`, logging); image Jib distroless Java 21; package `com.pangate.auth`.

**Cổng:**

| Cổng | Nội dung | Ai được gọi |
|---|---|---|
| `:8090` (public, qua API ingress) | `POST /oauth2/token`, `POST /v1/tokens`, `POST /v1/revocations`, `GET /.well-known/jwks.json` | Tenant backend, gateway, Push API |
| `:8081` (quản trị, chỉ nội bộ) | `/admin/v1/**`, Actuator (`/actuator/health/readiness` hiển thị tại `/ready`, `/live`, `/actuator/prometheus`) | Ops, Admin Console sau này |

Cổng `8090` trùng `devtools issuer` có chủ đích: gateway profile `dev` chỉ đổi hostname trong `jwks-uri`.

**Cấu trúc package (hexagonal, §13.5):**

```
com.pangate.auth
├── AuthApplication
├── api/             TokenEndpoint (/oauth2/token), ConnectTokenController (/v1/tokens),
│                    RevocationController (/v1/revocations), JwksController, AdminController,
│                    ApiAuthFilter (Bearer → ApiPrincipal), ProblemHandler (RFC 9457 + lỗi OAuth2)
├── application/     IssueClientToken, MintConnectToken, Revoke, RotateSigningKeys, ExpireRevocations,
│                    ManageTenants, ManageClients
│   └── port/        TenantStore, ClientStore, SigningKeyStore, RevocationStore, Outbox, KeyCipher,
│                    TokenSigner, SecretHasher, Clock
├── domain/          Tenant, Client, Scope, SigningKey, KeyState, Revocation, ConnectTokenRequest,
│                    ConnectClaims, ApiClaims, ClaimLimits   (Java thuần, không Spring)
├── infrastructure/
│   ├── postgres/    Jdbc*Store, AdvisoryLock, Flyway migrations (src/main/resources/db/migration)
│   ├── kafka/       OutboxRelay, ControlEventMapper
│   ├── crypto/      Es256TokenSigner (Nimbus), AesGcmKeyCipher, Argon2SecretHasher
│   └── ratelimit/   TokenBuckets
└── config/          AuthProperties (@ConfigurationProperties("auth")), SeedRunner, JobsConfig
```

Domain và use case test được không cần Spring. Controller chỉ map HTTP ↔ use case.

---

## 2. Mô hình dữ liệu (PostgreSQL, schema `auth`)

Migration Flyway `V1__init.sql`:

```sql
CREATE TABLE tenants (
  id          text PRIMARY KEY CHECK (id ~ '^[a-z0-9][a-z0-9_-]{0,63}$' AND id <> '__warmup'),
  status      text NOT NULL CHECK (status IN ('active','disabled')),
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE clients (
  id                text PRIMARY KEY,
  tenant_id         text NOT NULL REFERENCES tenants(id),
  secret_hash       text NOT NULL,          -- Argon2id, encoded (tham số nằm trong chuỗi)
  secret_hash_next  text,                   -- secret mới trong lúc xoay; cả hai đều hợp lệ
  scopes            text[] NOT NULL,
  status            text NOT NULL CHECK (status IN ('active','disabled')),
  created_at        timestamptz NOT NULL DEFAULT now(),
  secret_rotated_at timestamptz
);

CREATE TABLE signing_keys (
  kid              text PRIMARY KEY,         -- base64url(SHA-256 thumbprint) theo RFC 7638
  private_key_enc  bytea NOT NULL,           -- AES-256-GCM(KEK): 12 B nonce || ciphertext || tag
  kek_id           text NOT NULL,            -- tên KEK đã dùng, để xoay KEK sau này
  public_jwk       jsonb NOT NULL,
  state            text NOT NULL CHECK (state IN ('pending','active','retired')),
  created_at       timestamptz NOT NULL DEFAULT now(),
  activate_at      timestamptz NOT NULL,
  retire_at        timestamptz,              -- lúc chuyển sang retired
  remove_at        timestamptz               -- retire_at + 7 ngày: gỡ khỏi JWKS và xoá
);
CREATE UNIQUE INDEX one_active_key ON signing_keys ((state)) WHERE state = 'active';

CREATE TABLE revocations (
  id          bigserial PRIMARY KEY,
  tenant_id   text NOT NULL,
  kind        text NOT NULL CHECK (kind IN ('principal','device','jti','client')),
  control_key text NOT NULL,                 -- khoá trên gw.control, ví dụ revoke:p:t1:u1
  expires_at  timestamptz NOT NULL,
  created_by  text NOT NULL,                 -- client_id của người gọi
  created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX revocations_expiry ON revocations (expires_at);

CREATE TABLE outbox (
  id          bigserial PRIMARY KEY,
  topic       text NOT NULL,
  record_key  text NOT NULL,
  payload     bytea,                         -- NULL = tombstone
  created_at  timestamptz NOT NULL DEFAULT now(),
  sent_at     timestamptz
);
CREATE INDEX outbox_unsent ON outbox (id) WHERE sent_at IS NULL;
```

`client_id` có dạng `<tenant>-<tên>` (ví dụ `t1-backend`), duy nhất toàn hệ thống. Client của tenant hệ thống `pangate` (ví dụ `pangate-admin`) là client vận hành, không thuộc tenant khách hàng nào.

---

## 3. Khoá ký và JWKS

**Vòng đời** (thời lượng lấy từ cấu hình, mặc định theo §3.2):

| Trạng thái | Trên JWKS | Dùng để ký | Chuyển trạng thái |
|---|---|---|---|
| `pending` | Có | Không | → `active` khi `now ≥ activate_at` (tạo + 1 h, `auth.keys.publish-ahead`) |
| `active` | Có | Có (duy nhất) | → `retired` khi một key `pending` khác được kích hoạt (sau 30 ngày, `auth.keys.lifetime`) |
| `retired` | Có | Không | Xoá khi `now ≥ remove_at` (retire + 7 ngày, `auth.keys.overlap`) |

**Job `RotateSigningKeys`** (mỗi giờ, advisory lock `auth.rotate-keys`), mỗi lượt trong một transaction:
1. Không có key `active` (lần đầu chạy): tạo một key và kích hoạt ngay (`activate_at = now`). Token ký trước khi gateway kịp refresh JWKS sẽ đi nhánh `UnknownKid` → refresh khẩn cấp của `JwksCache` gateway (§4.9 spec gateway), nên vẫn qua.
2. Key `active` còn dưới 1 ngày tuổi thọ và chưa có key `pending`: tạo key `pending` với `activate_at = now + publish-ahead`.
3. Có key `pending` đã tới `activate_at`: `active` cũ → `retired` (`retire_at = now`, `remove_at = now + overlap`), `pending` → `active`.
4. Xoá các key `retired` đã quá `remove_at`.

Ràng buộc unique `one_active_key` là lớp bảo vệ thứ hai nếu advisory lock bị bỏ qua. Sau mỗi lượt có thay đổi, pod đó làm mới cache JWKS và cache khoá ký; các pod khác làm mới theo chu kỳ 60 s (`auth.keys.cache-refresh`), đủ nhanh vì key mới chỉ được dùng để ký sau 1 h.

**Mã hoá private key:** AES-256-GCM, nonce ngẫu nhiên 12 B, AAD = `kid`. KEK 32 byte đọc qua `SecretSource` với tên `auth.kek.<kek-id>`; `auth.keys.kek-id` chọn KEK dùng để mã hoá key mới, mọi KEK còn được tham chiếu phải đọc được khi khởi động.

**JWKS:** `GET /.well-known/jwks.json` trả `{"keys":[...]}` gồm các key `pending`, `active`, `retired` chưa bị xoá (`kty=EC, crv=P-256, alg=ES256, use=sig, kid, x, y`), header `Cache-Control: public, max-age=300`. Trả từ cache trong bộ nhớ; không truy vấn DB theo request.

---

## 4. API

Mọi endpoint public nhận và trả JSON UTF-8 (trừ `/oauth2/token` nhận `application/x-www-form-urlencoded`), body tối đa 16 KB.

### 4.1 `POST /oauth2/token` (client credentials, RFC 6749 §4.4)

- Xác thực client: HTTP Basic (`client_id:client_secret`, URL-encoded theo RFC 6749 §2.3.1) hoặc `client_id`/`client_secret` trong form. Có cả hai cách thì `400 invalid_request`.
- `grant_type` khác `client_credentials` → `400 unsupported_grant_type`.
- Client và tenant phải `active`; secret khớp `secret_hash` hoặc `secret_hash_next` (Argon2id, so sánh hằng thời gian). Sai → `401 invalid_client` kèm `WWW-Authenticate: Basic`. Client không tồn tại vẫn chạy một lượt Argon2id giả để không lộ qua thời gian phản hồi.
- `scope` (chuỗi cách nhau bằng dấu cách): bỏ trống thì nhận đủ scope được cấp; có scope ngoài danh sách được cấp → `400 invalid_scope`.
- Trả `200 {"access_token","token_type":"Bearer","expires_in":900,"scope"}`, `Cache-Control: no-store`.

**Claim token API:**

| Claim | Giá trị |
|---|---|
| header `alg`, `kid` | `ES256`, kid của key `active` |
| `iss` | `auth.issuer` (prod `https://auth.pangate`) |
| `aud` | `pangate-api` |
| `sub` | `client:<client_id>` |
| `tenant` | tenant của client |
| `client_id` | id client |
| `scope` | các scope đã cấp, cách nhau bằng dấu cách |
| `jti` | UUID ngẫu nhiên |
| `iat`, `nbf`, `exp` | `now`, `now`, `now + 900 s` (`auth.api-token-ttl`) |

**Scope:** `tokens:mint`, `sessions:revoke`, `push:send`, `push:broadcast`, `presence:read`, `devices:write` (§3.6), và `admin` chỉ dành cho client của tenant `pangate`. Admin API từ chối mọi token không có `admin`, và Auth không bao giờ cấp `admin` cho client của tenant khác (kiểm tra cả khi tạo client lẫn khi cấp token).

### 4.2 `ApiAuthFilter` (cho `/v1/**` và `/admin/v1/**`)

`Authorization: Bearer <token API>` được kiểm tra bằng chính bộ key của Auth: chữ ký, `aud=pangate-api`, `iss`, `exp`/`nbf` (lệch đồng hồ 30 s), `jti` chưa bị thu hồi và `client_id` chưa bị vô hiệu hoá (đọc từ cache 10 s của bảng `clients` và `revocations`). Thiếu hoặc sai → `401` (`WWW-Authenticate: Bearer error="invalid_token"`); thiếu scope → `403`.

### 4.3 `POST /v1/tokens` (scope `tokens:mint`)

Body: `{"principal": "t1:u123", "device": "d1", "topics": ["t1:news.*"], "ttlSec": 600}`.

Ràng buộc (`ClaimLimits`, khớp `JwtVerifier` và `ResumeTicket` của gateway); vi phạm → `400` Problem+JSON với `code` tương ứng:

| Luật | `code` |
|---|---|
| `tenant` lấy từ token API, không lấy từ body; tenant phải `active` | `tenant_disabled` (403) |
| `principal` bắt đầu bằng `<tenant>:`, tổng ≤ 256 byte UTF-8 | `principal_outside_tenant`, `principal_too_long` |
| `device` không rỗng, ≤ 128 byte | `device_invalid` |
| `topics` ≤ 8 phần tử; mỗi phần tử bắt đầu bằng `<tenant>:` và ≤ 64 byte | `too_many_topics`, `topic_outside_tenant`, `topic_pattern_too_long` |
| `ttlSec` trong [60, 600], mặc định 600 | `ttl_out_of_range` |

Trả `200 {"token": "...", "expiresAt": "<ISO-8601>"}`. Claim: header `ES256` + `kid`; `iss`, `aud=pangate-gw`, `sub=<principal>`, `tenant`, `device`, `topics` (mảng, có thể rỗng), `jti` (UUID), `iat`, `nbf=now`, `exp=now+ttlSec`. Tenant `pangate` không được mint connect token (`403 tenant_not_allowed`).

### 4.4 `POST /v1/revocations` (scope `sessions:revoke`)

Body: một trong
- `{"kind":"principal","principal":"t1:u123"}` → khoá `revoke:p:t1:u123`, `Revoke{principal_id}`
- `{"kind":"device","principal":"t1:u123","device":"d1"}` → khoá `revoke:d:t1:u123/d1`, `Revoke{device}`
- `{"kind":"jti","jti":"<jti của connect token>"}` → khoá `revoke:j:<jti>`, `Revoke{jti}`

`principal` phải thuộc tenant của người gọi (`403 principal_outside_tenant`). Thu hồi theo `jti` không kiểm tra được tenant (jti không mang tenant), nên chỉ client có scope `sessions:revoke` mới gọi được và bản ghi lưu `created_by` để truy vết.

Trong **một transaction**: insert `revocations` (`expires_at = now + 24 h`, `auth.revocation-ttl`, bằng TTL dài nhất của resume ticket) và insert `outbox` (`topic = gw.control`, `record_key = control_key`, payload `ControlEvent{revoke, issued_at_ms}` với `Revoke.expires_at_ms`). Thu hồi trùng khoá còn hiệu lực thì cập nhật `expires_at` và ghi outbox mới (gia hạn). Trả `202 {"id", "controlKey", "expiresAt"}`.

### 4.5 Admin API (`:8081/admin/v1`, scope `admin`)

| Method | Đường dẫn | Tác dụng |
|---|---|---|
| `POST` | `/tenants` `{id}` | Tạo tenant `active` |
| `GET` | `/tenants`, `/tenants/{t}` | Liệt kê, xem |
| `POST` | `/tenants/{t}:disable`, `:enable` | Tenant `disabled`: không cấp token API, không mint connect token cho tenant đó |
| `POST` | `/tenants/{t}/clients` `{name, scopes[]}` | Tạo client `<t>-<name>`, trả secret dạng rõ **một lần** |
| `GET` | `/tenants/{t}/clients` | Liệt kê (không bao giờ trả secret hay hash) |
| `POST` | `/clients/{id}:rotate-secret` | Sinh secret mới vào `secret_hash_next`, trả secret một lần |
| `POST` | `/clients/{id}:finish-rotation` | `secret_hash = secret_hash_next`, xoá `secret_hash_next` |
| `POST` | `/clients/{id}:disable` | `status = disabled` + một thu hồi kind `client` (khoá `revoke:c:<client_id>`, xem §4.6) để Push API chặn token đã cấp ngay |
| `POST` | `/keys:rotate-now` | Tạo key `pending` mới ngay (sự cố lộ key); vẫn tuân theo `publish-ahead` |

Mọi lệnh Admin ghi log audit có cấu trúc: `admin_action`, `actor` (client_id), `target`, `result`.

### 4.6 Thay đổi contract (additive)

`Revoke` trong `platform-contracts/proto/pangate/v1/control.proto` thêm `string client_id = 5;` vào `oneof target`, cùng khoá `revoke:c:<client_id>` vào bảng khoá ở đầu file. Push API (sub-project 4) dùng nó để chặn token của client đã bị vô hiệu hoá. `buf breaking` phải xanh.

Gateway cần một thay đổi nhỏ: hiện `ControlKeys` trả `Unknown` cho tiền tố lạ và `ControlEventConsumer` đếm nó vào `badRecords`, nên mỗi lần vô hiệu hoá client sẽ thành một bản ghi "hỏng" giả. `ControlKeys` nhận diện `revoke:c:` thành `ClientRevokeKey` và consumer bỏ qua nó (không đếm `badRecords`), có unit test.

---

## 5. Job nền

Mọi job chạy trên scheduler của Spring với virtual threads, mỗi job một PostgreSQL advisory lock (`pg_try_advisory_xact_lock`), nên chạy nhiều pod vẫn chỉ một pod làm mỗi lượt.

| Job | Chu kỳ | Việc làm |
|---|---|---|
| `RotateSigningKeys` | 1 h (và một lần lúc khởi động) | §3 |
| `OutboxRelay` | 200 ms | Đọc tối đa 500 dòng `sent_at IS NULL` theo `id` (`FOR UPDATE SKIP LOCKED`); produce lần lượt với producer idempotent `acks=all`, chờ ack; đánh dấu `sent_at`. Lỗi Kafka: dừng lượt này, lượt sau gửi lại từ dòng chưa gửi đầu tiên, nên thứ tự theo `id` được giữ. Xoá dòng đã gửi quá 7 ngày |
| `ExpireRevocations` | 60 s | Với mỗi thu hồi quá `expires_at`: insert outbox tombstone (`payload NULL`, cùng `record_key`) và xoá dòng, trong một transaction |

Headers trên mọi record theo quy ước §3.4: `traceparent` (nếu có), `tenant-id`, `content-type: application/x-protobuf; schema=pangate.v1.ControlEvent`, qua `PangateHeaders` của starter.

---

## 6. Xử lý lỗi

**Định dạng.** `/oauth2/token` trả lỗi OAuth2 chuẩn (`{"error","error_description"}`). Các endpoint khác trả `application/problem+json` (RFC 9457): `type`, `title`, `status`, `detail`, `code`. Không trả stack trace hay thông tin nội bộ.

| Sự cố | Hành vi |
|---|---|
| PostgreSQL sập | Endpoint cần DB trả `503` + `Retry-After: 5`; JWKS vẫn trả 200 từ cache; `/ready` → 503 (health group `db`); `/live` vẫn 200 |
| Kafka sập | `POST /v1/revocations` vẫn `202`; outbox dồn lại; metric `auth.outbox.oldest_age_seconds` cảnh báo khi > 30 s; relay gửi bù đúng thứ tự khi Kafka trở lại; `/ready` không phụ thuộc Kafka |
| KEK thiếu khi khởi động | Fail fast bằng `FailureAnalyzer`: tên secret và nơi tìm (`SecretSource.describe`) |
| Không giải mã được một key (KEK sai) | Bỏ qua key đó khi ký, log `error`, metric `auth.keys.undecryptable`; nếu key `active` không giải mã được thì job xoay tạo và kích hoạt key mới ngay; chỉ báo `/ready` 503 khi không còn key nào ký được |
| Hai pod cùng xoay key | Advisory lock; unique index `one_active_key` chặn lớp thứ hai (transaction thua rollback) |
| Body quá 16 KB / JSON sai | `413` / `400 malformed_request` |

---

## 7. Giới hạn và bảo mật

**Rate limit** (token bucket trong bộ nhớ mỗi pod, xấp xỉ như §3.2; vượt → `429` + `Retry-After`):

| Endpoint | Khoá | Mặc định |
|---|---|---|
| `/oauth2/token` | `client_id` | 10/s, burst 20 |
| `/oauth2/token` thất bại xác thực | IP nguồn | > 20 lần/phút → chặn IP 60 s |
| `/v1/tokens` | `client_id` | 1000/s, burst 2000 |
| `/v1/revocations` | `client_id` | 100/s, burst 200 |
| `/admin/v1/**` | `client_id` | 20/s |

**Bảo mật:**
- Secret client: 32 byte ngẫu nhiên, base64url; chỉ lưu Argon2id (OWASP: m=19 MiB, t=2, p=1); trả dạng rõ một lần.
- Private key chỉ giải mã trong bộ nhớ, không ghi log, không xuất qua API.
- Log và trace không chứa token, secret, body request; `principal` được hash bằng `PrincipalHasher` của starter.
- Tenant chỉ thao tác trong namespace của mình (`principal`, `topics`, thu hồi theo principal/device); tenant `__warmup` bị cấm ở DB lẫn ở API.
- Admin API chỉ mở trên `:8081`; trong Kubernetes, `:8081` không nằm sau ingress.
- Không có cookie; không cần CSRF. CORS tắt.

**Metrics** (Micrometer, tag chung của starter): `auth.tokens.issued{type=api|connect, result}`, `auth.token.latency{type}`, `auth.revocations{kind}`, `auth.keys{state}`, `auth.keys.undecryptable`, `auth.outbox.pending`, `auth.outbox.oldest_age_seconds`, `auth.ratelimited{endpoint}`, `auth.admin.actions{action, result}`.

---

## 8. Cấu hình và profile

`@ConfigurationProperties("auth")`, có giá trị mặc định:

```yaml
auth:
  issuer: https://auth.pangate
  api-token-ttl: 15m
  connect-token-max-ttl: 10m
  revocation-ttl: 24h
  keys: { lifetime: 30d, overlap: 7d, publish-ahead: 1h, kek-id: v1, cache-refresh: 60s }
  outbox: { interval: 200ms, batch: 500, keep-sent: 7d }
  kafka: { control-topic: gw.control }
  seed: { enabled: false }         # local/dev: true
```

| Profile | Postgres | Kafka | KEK, seed | Ghi chú |
|---|---|---|---|---|
| `local` | `localhost:5432` (từ `make infra-up`) | `localhost:9092` | KEK và secret seed sinh tự động vào `.dev/auth/` lần đầu chạy (giống `DevBootstrap` của gateway); seed bật | `issuer: http://localhost:8090`; chạy từ IDE hoặc `make auth-local` |
| `dev` | service `postgres` trong compose | `kafka:29092` | KEK mount từ volume dev; seed bật | `issuer: http://auth:8090`; container trong `make up` |
| `prod` | Theo môi trường | Theo môi trường | KEK từ Vault qua `SecretSource`; seed tắt; client admin đầu tiên tạo bằng runbook | Image Jib |

Seed (idempotent): tenant `pangate` + client `pangate-admin` (scope `admin`); tenant `t1` + client `t1-backend` (scope `tokens:mint sessions:revoke push:send push:broadcast`). Secret seed ghi vào `.dev/auth/clients.json` để devtools đọc.

---

## 9. Thay đổi ngoài `auth-service`

| Nơi | Thay đổi |
|---|---|
| `platform-contracts` | `Revoke.client_id` + khoá `revoke:c:` (§4.6) |
| `platform-devenv/docker-compose.yml` | Thêm `postgres` (16, healthcheck), thêm `auth` (profile `app`), gỡ `issuer` khỏi profile `app` |
| `gateway-service` (code) | `ControlKeys` nhận `revoke:c:` và consumer bỏ qua nó (§4.6); `pom.xml` publish `test-jar` (cho contract test của Auth) |
| `Makefile` | `MODULES` thêm `auth-service` **sau** `gateway-service` (contract test cần test-jar của gateway); `infra-up` khởi động cả `postgres`; thêm `auth-local`, `auth-local-check`; `images` build cả image auth |
| `gateway-service` (chỉ cấu hình) | `application-dev.yml`: `jwks-uri: http://auth:8090/.well-known/jwks.json`, `issuer: http://auth:8090`; `application.yml` (prod): `issuer: https://auth.pangate`. Profile `local` giữ JWKS từ file của `DevBootstrap`, để gateway vẫn chạy được khi không có Auth |
| `gateway-devtools` | `smoke` và `token` thêm `--auth-url` và `--client-file` (đọc `.dev/auth/clients.json`): lấy token API rồi gọi `/v1/tokens`; `smoke` thêm bước revoke. `issuer` giữ lại cho IT của gateway và profile `local` |
| `.github/workflows/ci.yml` | Job `build` chạy `make verify` (đã gồm module mới); `dev-start` chạy smoke qua Auth |
| `platform-devenv/scripts/release-check.sh` | Kiểm version `auth-service` |
| `docs/adr/` | ADR 0013–0015 (§11) |

---

## 10. Kiểm thử

| Mức | Nội dung tối thiểu |
|---|---|
| Unit (không Spring) | `ClaimLimits` (mọi luật ở §4.3, biên đúng/sai); scope (subset, `admin` chỉ cho tenant `pangate`); máy trạng thái key với đồng hồ giả (lần đầu, publish trước, kích hoạt, retire, xoá); `AesGcmKeyCipher` (khứ hồi, sai KEK, sai AAD); `Argon2SecretHasher`; token bucket; `ControlEventMapper` (khoá và payload cho cả bốn kind, tombstone) |
| Contract | Connect token do `Es256TokenSigner` mint được `JwtVerifier` của gateway chấp nhận, và token API bị nó từ chối (`wrong audience`). Gateway publish `test-jar` chứa `JwtVerifier` và `JwksCache`; `auth-service` dùng nó ở scope `test` (không phải phụ thuộc runtime, nên không vi phạm §13.7) |
| Integration (Testcontainers Postgres 16 + Kafka) | Mọi endpoint với mã lỗi ở §4 và §6; outbox → `gw.control` đúng thứ tự, Kafka tạm dừng rồi chạy lại vẫn đủ và đúng thứ tự; tombstone khi hết hạn; hai context Spring cùng DB chỉ một bên xoay key; Postgres dừng thì `/.well-known/jwks.json` vẫn 200 và `/oauth2/token` trả 503; rate limit trả 429 |
| E2E (devenv, `make smoke`) | Token API → `/v1/tokens` → HELLO vào gateway → push → ACK; sau đó `POST /v1/revocations` principal đó → gateway đóng session 4003 trong < 10 s |

---

## 11. ADR mới

| ADR | Quyết định |
|---|---|
| 0013 | Auth tự quản lý khoá ES256 trong PostgreSQL: KEK qua `SecretSource`, publish trước 1 h, chồng lấn 7 ngày, advisory lock |
| 0014 | Token API và connect token dùng chung bộ key, tách nhau bằng `aud` (`pangate-api` / `pangate-gw`) |
| 0015 | Thu hồi qua transactional outbox lên `gw.control`; tombstone khi hết hạn |

---

## 12. Milestone và tiêu chí hoàn thành

Một milestone (một implementation plan), **A1**:

1. Unit, contract và IT của `auth-service` xanh; `buf breaking` xanh với `Revoke.client_id`.
2. Auth khởi động được ở `local` (Postgres trên localhost, KEK sinh tự động) và `dev` (compose).
3. Gateway profile `dev` chỉ dùng JWKS của Auth; `devtools issuer` không còn trong profile `app` của compose.
4. `make smoke` xanh qua Auth, gồm cả bước revoke (4003 trong < 10 s).
5. IT diễn tập xoay key với đồng hồ tua nhanh: token ký bằng key cũ vẫn được chấp nhận suốt thời gian chồng lấn, token mới ký bằng key mới, JWKS luôn chứa cả hai.
6. Khi Postgres dừng, JWKS vẫn 200 và gateway vẫn nhận HELLO bằng token đã mint trước đó.
7. CI xanh: `build`, `contracts-lint`, `local-start`, `dev-start`.

---

## 13. Câu hỏi còn mở

1. Giá trị `auth.issuer` thật ở prod (`https://auth.pangate` là giá trị tạm) phụ thuộc domain của API ingress.
2. Khi có Vault thật, cách Vault cấp KEK cho `SecretSource` (KV v2 hay Transit `datakey`) cần thống nhất với team Security; spec này chỉ cần `SecretSource.get("auth.kek.<id>")` trả 32 byte.
