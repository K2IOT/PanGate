# account-service (Tài khoản, CH2) — Design Spec

- **Ngày:** 2026-11-16
- **Trạng thái:** Đã hiện thực (CH2, plan `docs/superpowers/plans/2026-11-16-account-service-ch2.md`).
- **Các spec liên quan:**
  - "spec chat": `docs/superpowers/specs/2026-11-12-chat-core-design.md`: tenant `chat`, principal `chat:<userId>`, HTTP API của chat-service kiểm connect token, Revocations từ `gw.control`.
  - "spec auth": `docs/superpowers/specs/2026-11-05-auth-service-design.md`: token API (`/oauth2/token`, `aud=pangate-api`), `POST /v1/tokens` (connect token, `aud=pangate-gw`, tối đa 10 phút), `POST /v1/revocations` (principal / device / jti, có hiệu lực với token cấp **trước** lệnh thu hồi, TTL 24 h), outbox → Kafka.
- **Vị trí trong lộ trình app chat:** sub-project 2/4: (CH1) Chat core, **(CH2) Tài khoản**, (CH3) Trạng thái hội thoại, (CH4) Device Registry và Offline Delivery.

---

## 0. Mục tiêu, phạm vi

**Mục tiêu.** App chat (kiểu Telegram, cho người dùng cuối, mục tiêu hàng chục triệu user) cần người dùng thật thay cho connect token do devtools tạo. CH2 là "backend của tenant `chat`" theo nghĩa của spec auth: nó giữ danh tính user, mật khẩu và phiên đăng nhập của từng thiết bị, và là nơi duy nhất xin auth-service cấp connect token cho `chat:<userId>`.

**Quyết định đã chốt khi brainstorm (2026-11-16):**

| # | Câu hỏi | Quyết định |
|---|---|---|
| A1 | Đăng nhập bằng gì | Email + mật khẩu, hoặc số điện thoại + mật khẩu. OTP để sau |
| A2 | Tìm nhau bằng gì | Username (duy nhất, đổi được). Email/số điện thoại chưa xác minh nên chưa dùng để tìm bạn |
| A3 | Vận hành có can thiệp không | Có: khoá / mở khoá tài khoản, đặt lại mật khẩu, xoá tài khoản |
| A4 | Xoá tài khoản thì tin nhắn đã gửi ra sao | Giữ tin, người gửi hiện là "Tài khoản đã xoá" (như Telegram) |
| A5 | Đặt ở đâu | Service mới `account-service`, dữ liệu trên PostgreSQL (hướng A, §1) |

**Tiêu chí thành công.**
- Devtools đăng ký hai tài khoản, đăng nhập, đổi refresh token lấy connect token, vào gateway và chat 1-1 qua chat-service như CH1 (`make smoke` có bước này).
- Đăng xuất một thiết bị: gateway đóng phiên của thiết bị đó (4003) trong < 10 s; thiết bị khác của cùng user vẫn chạy.
- Admin khoá tài khoản: mọi phiên đóng trong < 10 s, đăng nhập và đổi token bị từ chối; mở khoá thì đăng nhập lại được.
- Xoá tài khoản: user rời mọi nhóm và kênh, DM với người đó không gửi thêm được, hồ sơ trả `deleted: true`, username và email/số điện thoại dùng lại được cho tài khoản mới.
- Đổi mật khẩu: các thiết bị khác bị đăng xuất.

**Ngoài phạm vi CH2:**
- OTP, xác minh email / số điện thoại, quên mật khẩu tự phục vụ, tìm bạn qua danh bạ (cần xác minh trước);
- avatar (ảnh/file nằm ngoài lộ trình bản đầu), 2FA, đăng nhập bằng Google/Apple, tài khoản bot;
- đẩy hồ sơ thay đổi (đổi tên, đổi username) tới client đang mở: client tự tải lại hồ sơ khi cần.

**Không được xảy ra (anti-goals):**
- Mật khẩu, refresh token, connect token xuất hiện ở dạng rõ trong log, trace, DB hoặc metric.
- Lỗi đăng nhập cho biết tài khoản có tồn tại hay không: sai login và sai mật khẩu trả cùng một lỗi.
- Một `userId` được cấp lại cho người khác: tin cũ sẽ trỏ nhầm người.
- Tài khoản bị khoá hoặc đã xoá vẫn lấy được connect token mới.

---

## 1. Kiến trúc và hướng tiếp cận

**Các hướng đã cân nhắc:**

| Hướng | Được | Mất | Chọn |
|---|---|---|---|
| **A. Service mới `account-service`, PostgreSQL** | Username, email, số điện thoại duy nhất cùng lúc trong một transaction; theo khuôn auth-service (Flyway, outbox, advisory lock); vài chục triệu dòng là mức PostgreSQL xử lý được | Thêm một service và một schema PostgreSQL để vận hành | **Có** |
| B. Gộp vào chat-service, ScyllaDB | Ít service | Duy nhất ba khoá cùng lúc phải ghép nhiều LWT, dễ dở dang; lõi chat trộn với mật khẩu và phiên | Không |
| C. Mở rộng auth-service | Có sẵn PostgreSQL, ký token | auth-service là dịch vụ token của nền tảng, đa tenant; user cuối của riêng app chat phá ranh giới đó | Không |

**Vị trí trong hệ thống:**

```
client ──HTTP──▶ account-service ──client credentials──▶ auth-service  (/oauth2/token, /v1/tokens, /v1/revocations)
   │                   │
   │                   └─outbox─▶ Kafka account.users (compacted) ─▶ chat-service (bảng users, dọn thành viên khi xoá)
   │
   └─connect token──▶ gateway (WebSocket) / chat-service (HTTP) / account-service (HTTP)
```

`account-service/` ở gốc repo, parent `platform-parent`, phụ thuộc `platform-contracts` và `platform-spring-starter`; image Jib distroless Java 21; package `com.pangate.account`; bố cục hexagonal như auth-service (`api/`, `application/` + `port/`, `domain/` Java thuần, `infrastructure/{postgres,kafka,crypto,auth,ratelimit}`, `config/`).

**Cổng:**

| Cổng | Nội dung | Ai gọi |
|---|---|---|
| `:8080` (public, qua API ingress) | `/v1/**` (§4) | Client |
| `:8081` (chỉ nội bộ) | `/admin/v1/**` (§5), Actuator (`/ready`, `/live`, `/actuator/prometheus`) | Ops, Admin Console sau này |

**Phụ thuộc lúc chạy:** PostgreSQL (dữ liệu, schema `account`), Redis (đếm đăng nhập sai và rate limit dùng chung giữa các pod), Kafka (`account.users`), auth-service (token API, mint connect token, thu hồi, JWKS).

---

## 2. Token và phiên

Một **phiên** là một thiết bị đã đăng nhập. `deviceId` của phiên do account-service cấp (ULID) lúc đăng nhập, và là `device` trong connect token. Đăng nhập lại trên cùng máy tạo phiên mới với `deviceId` mới. CH4 sẽ gắn push token vào `deviceId` này.

| Token | Ai cấp | Sống bao lâu | Dùng cho |
|---|---|---|---|
| **Refresh token** | account-service, lúc đăng nhập | Tới khi đăng xuất, hoặc không dùng quá 180 ngày (`account.session-idle-ttl`) | Chỉ để đổi lấy connect token |
| **Connect token** | auth-service (`POST /v1/tokens`), account-service xin hộ | 10 phút | Gateway (WebSocket), HTTP API của chat-service, HTTP API của account-service |

- Refresh token: 32 byte ngẫu nhiên, base64url, tiền tố `rt_`. Chỉ lưu SHA-256 của nó (đủ, vì giá trị có 256 bit ngẫu nhiên). Không xoay vòng mỗi lần dùng: một máy dùng một refresh token suốt phiên. Mỗi lần đổi token cập nhật `last_used_at` (ghi gộp, tối đa một lần mỗi 5 phút mỗi phiên).
- Connect token: account-service gọi `POST /v1/tokens` với `principal = chat:<userId>`, `device = <deviceId>`, `topics = []`, `ttlSec = 600`, bằng token API của client `chat-backend` (scope `tokens:mint`), token API được cache tới gần hết hạn.
- Mọi API của account-service cần đăng nhập (trừ đăng ký, đăng nhập, đổi token) nhận `Authorization: Bearer <connect token>`, kiểm offline qua JWKS của auth-service giống chat-service: chữ ký, `aud=pangate-gw`, `iss`, `exp` (lệch 30 s), `tenant = chat`, và chưa bị thu hồi theo jti / device / principal (đọc `gw.control`). Sau đó kiểm thêm phiên `deviceId` còn sống trong DB (cache 10 s), để API của chính account-service không phải chờ connect token hết hạn sau khi đăng xuất.
- **Thu hồi theo thiết bị, không theo user.** Gateway chặn một lệnh thu hồi `principal` hoặc `device` suốt TTL 24 h với **mọi** token, kể cả token cấp sau đó (`RevocationList.isRevoked` không xét `iat`; chỉ chat-service xét). Vì `deviceId` không bao giờ dùng lại, thu hồi `device` chỉ chặn đúng phiên cũ. Nên:
  - đăng xuất một thiết bị, đăng xuất mọi thiết bị (khác), đổi mật khẩu, đặt lại mật khẩu, **khoá**: đánh dấu từng phiên `revoked_at` và xin thu hồi `device` cho từng phiên đó; đăng nhập lại tạo `deviceId` mới, không bị chặn; mở khoá có tác dụng ngay;
  - chỉ **xoá tài khoản** thu hồi `principal` (user đó không bao giờ đăng nhập lại, nên chặn 24 h là đúng ý);
  - đổi mật khẩu không thu hồi phiên hiện tại, nên không có cuộc đua giữa lệnh thu hồi và token mới của thiết bị đang dùng.
- Lệnh thu hồi gửi tới auth-service qua outbox của account-service (§6): ghi DB và hàng chờ trong cùng transaction, job gửi tới khi auth-service trả `202`. Mất auth-service tạm thời không làm mất lệnh thu hồi; trong lúc đó account-service đã từ chối đổi token cho phiên bị thu hồi, chỉ connect token đang cầm (≤ 10 phút) còn dùng được ở gateway.

---

## 3. Mô hình dữ liệu (PostgreSQL, schema `account`)

Migration Flyway `V1__init.sql`:

```sql
CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE accounts (
  user_id              text PRIMARY KEY,               -- ULID, không bao giờ cấp lại
  username             citext UNIQUE,                  -- NULL khi đã xoá
  display_name         text,                           -- 1..64 ký tự; NULL khi đã xoá
  bio                  text,                           -- ≤ 140 ký tự
  password_hash        text,                           -- Argon2id encoded; NULL khi đã xoá
  must_change_password boolean NOT NULL DEFAULT false,
  status               text NOT NULL CHECK (status IN ('active','locked','deleted')),
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now(),
  password_changed_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at           timestamptz
);

CREATE TABLE identifiers (
  kind       text NOT NULL CHECK (kind IN ('email','phone')),
  value      text NOT NULL,                            -- đã chuẩn hoá (§4.1)
  user_id    text NOT NULL REFERENCES accounts(user_id),
  verified   boolean NOT NULL DEFAULT false,           -- CH2 luôn false; OTP sau này bật
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (kind, value),
  UNIQUE (user_id, kind)                               -- tối đa một email và một số điện thoại mỗi tài khoản
);

CREATE TABLE sessions (
  device_id     text PRIMARY KEY,                      -- ULID
  user_id       text NOT NULL REFERENCES accounts(user_id),
  refresh_hash  bytea NOT NULL UNIQUE,                 -- SHA-256(refresh token)
  device_name   text NOT NULL,                         -- do client gửi, ≤ 64 ký tự, ví dụ "iPhone của Lan"
  platform      text NOT NULL CHECK (platform IN ('ios','android','web','desktop','other')),
  created_at    timestamptz NOT NULL DEFAULT now(),
  last_used_at  timestamptz NOT NULL DEFAULT now(),
  revoked_at    timestamptz
);
CREATE INDEX sessions_by_user ON sessions (user_id) WHERE revoked_at IS NULL;

CREATE TABLE admin_actions (
  id         bigserial PRIMARY KEY,
  actor      text NOT NULL,                            -- client_id trong token API
  action     text NOT NULL,                            -- lock | unlock | reset_password | delete
  user_id    text NOT NULL,
  reason     text NOT NULL,                            -- bắt buộc, ≤ 500 ký tự
  at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE outbox (                                  -- như outbox của auth-service
  id          bigserial PRIMARY KEY,
  kind        text NOT NULL CHECK (kind IN ('event','revocation')),
  record_key  text NOT NULL,                           -- user_id
  payload     bytea NOT NULL,                          -- AccountState (Protobuf) hoặc body JSON của /v1/revocations
  created_at  timestamptz NOT NULL DEFAULT now(),
  sent_at     timestamptz
);
CREATE INDEX outbox_pending ON outbox (id) WHERE sent_at IS NULL;
```

Phiên bị thu hồi và phiên quá hạn không dùng được xoá sau 30 ngày (job, §6).

---

## 4. API công khai (`:8080`, JSON, lỗi `application/problem+json` với `code` ổn định)

### 4.1 Quy tắc dữ liệu

| Trường | Quy tắc | Lỗi |
|---|---|---|
| `username` | `^[a-zA-Z][a-zA-Z0-9_]{4,31}$`, so sánh không phân biệt hoa thường, giữ nguyên cách viết người dùng chọn để hiển thị; danh sách từ cấm (`admin`, `support`, `pangate`, …, cấu hình được) | `invalid_username`, `username_taken` |
| `email` | Cắt khoảng trắng, chữ thường; một `@`, phần tên miền có dấu chấm; ≤ 254 ký tự. Không gộp kiểu Gmail (dấu chấm, `+tag`) | `invalid_email`, `email_taken` |
| `phone` | Client gửi dạng E.164 `^\+[1-9][0-9]{6,14}$`; server không tự đoán mã quốc gia | `invalid_phone`, `phone_taken` |
| `password` | 8..128 ký tự (Unicode, chuẩn hoá NFC); không trùng username, email hoặc số điện thoại | `weak_password` |
| `displayName` | 1..64 ký tự sau khi cắt khoảng trắng, không ký tự điều khiển; mặc định = username | `invalid_display_name` |
| `bio` | ≤ 140 ký tự | `invalid_bio` |

### 4.2 Endpoint

| Endpoint | Xác thực | Mô tả |
|---|---|---|
| `POST /v1/accounts` | Không | Body `{username, password, email?, phone?, displayName?, deviceName, platform}`; cần ít nhất một trong `email`, `phone`. Tạo tài khoản, identifier, phiên đầu tiên và bản `AccountState` `ACTIVE` trong **một** transaction. `201` như kết quả đăng nhập |
| `POST /v1/sessions` | Không | Đăng nhập. Body `{login, password, deviceName, platform, newPassword?}`; `login` là email, số điện thoại (bắt đầu bằng `+`) hoặc username. `201 {userId, deviceId, refreshToken}`. Sai login hoặc mật khẩu: `401 invalid_credentials` (cùng một lỗi, cùng thời gian xử lý). Bị khoá: `403 account_locked`. Đã xoá: như sai login. `must_change_password` mà không có `newPassword`: `403 password_change_required`; có `newPassword` hợp lệ thì đổi mật khẩu rồi đăng nhập |
| `POST /v1/sessions/token` | Refresh token (`Authorization: Bearer rt_…`) | `200 {connectToken, expiresAt}`. Phiên đã thu hồi / quá hạn, tài khoản khoá / xoá: `401 invalid_session` (khoá thì `403 account_locked`). auth-service không trả lời: `503` |
| `GET /v1/sessions` | Connect token | Các phiên còn sống của tôi: `deviceId`, `deviceName`, `platform`, `createdAt`, `lastUsedAt`, `current` |
| `DELETE /v1/sessions/{deviceId}` | Connect token | Đăng xuất một thiết bị của tôi (kể cả thiết bị hiện tại). `204` |
| `DELETE /v1/sessions?others=true` | Connect token | Đăng xuất mọi thiết bị khác thiết bị hiện tại. Không có `others`: đăng xuất tất cả. `204` |
| `GET /v1/me` | Connect token | Hồ sơ của tôi kèm `email`, `phone` (và `verified`), `createdAt` |
| `PATCH /v1/me` | Connect token | `{displayName?, bio?}` |
| `PUT /v1/me/username` | Connect token | `{username}`; username cũ được giải phóng ngay |
| `PUT /v1/me/email`, `PUT /v1/me/phone` | Connect token | `{value, password}` (giá trị `null` để gỡ, nhưng phải còn ít nhất một trong hai) |
| `POST /v1/me/password` | Connect token | `{currentPassword, newPassword}`; đăng xuất mọi thiết bị khác (thu hồi `device` từng phiên khác, §2); phiên hiện tại giữ nguyên |
| `DELETE /v1/me` | Connect token | `{password}`; xoá tài khoản (§5.3). `204` |
| `GET /v1/users/{userId}` | Connect token | Hồ sơ công khai `{userId, username, displayName, bio}`, hoặc `{userId, deleted: true}`. Tài khoản bị khoá vẫn hiện hồ sơ |
| `GET /v1/users?username=` | Connect token | Tìm đúng một username (không tìm gần đúng, không liệt kê). Không có: `404` |
| `POST /v1/users:batch` | Connect token | `{userIds: [...]}` (≤ 100) → hồ sơ công khai của từng id; để client hiện tên người gửi trong lịch sử chat. Id không tồn tại bị bỏ qua |

Đăng ký cho biết trường nào đã bị dùng (`username_taken`, `email_taken`, `phone_taken`): đây là trải nghiệm cần có, và được giới hạn bằng rate limit đăng ký (§7).

---

## 5. Admin API (`:8081/admin/v1`)

**Xác thực.** `Authorization: Bearer <token API>` của auth-service (`aud=pangate-api`), kiểm offline qua JWKS, cần scope mới `accounts:admin` và `tenant = chat`. auth-service thêm scope này vào `Scope` và seed client `chat-admin` (§9). Mọi thao tác ghi `admin_actions` (bắt buộc `reason`) và metric.

| Endpoint | Mô tả |
|---|---|
| `GET /admin/v1/accounts/{userId}` | Toàn bộ thông tin trừ mật khẩu: trạng thái, identifier, số phiên còn sống, lịch sử `admin_actions` |
| `GET /admin/v1/accounts?username=` / `?email=` / `?phone=` | Tra cứu đúng một tài khoản |
| `POST /admin/v1/accounts/{userId}:lock` | §5.1 |
| `POST /admin/v1/accounts/{userId}:unlock` | `status = active` và bản `AccountState` `ACTIVE`. Phiên cũ vẫn bị thu hồi; user đăng nhập lại từ đầu và kết nối được ngay (§2) |
| `POST /admin/v1/accounts/{userId}:reset-password` | §5.2. Trả `{temporaryPassword}` một lần duy nhất |
| `DELETE /admin/v1/accounts/{userId}` | §5.3 |

### 5.1 Khoá

Trong một transaction: `status = locked`, thu hồi mọi phiên (`revoked_at`), một hàng chờ thu hồi `device` cho mỗi phiên đó (§2), bản `AccountState` `LOCKED`. Kết quả: gateway đóng mọi kết nối (4003), chat-service và account-service từ chối connect token cũ ngay khi đọc được `gw.control`; đăng nhập và đổi token trả `403 account_locked`. Sau 24 h lệnh thu hồi hết hạn, nhưng phiên đã thu hồi và đăng nhập bị chặn nên user vẫn không lấy được token. Mở khoá có tác dụng ngay: đăng nhập tạo phiên mới. chat-service không cần biết trạng thái khoá.

### 5.2 Đặt lại mật khẩu

Sinh mật khẩu tạm 16 ký tự ngẫu nhiên, `must_change_password = true`, thu hồi mọi phiên (thu hồi `device` từng phiên, §2). Admin trao mật khẩu tạm cho user qua kênh ngoài hệ thống (CH2 chưa có email/SMS). Lần đăng nhập tiếp theo phải đổi mật khẩu (§4.2).

### 5.3 Xoá (do user hoặc admin)

Trong một transaction: `status = deleted`, `deleted_at = now()`; xoá dữ liệu cá nhân (`username`, `display_name`, `bio`, `password_hash` thành `NULL`, xoá hàng trong `identifiers`); thu hồi mọi phiên; hàng chờ thu hồi `principal`; bản `AccountState` `DELETED`. `user_id` giữ lại để tin nhắn cũ còn trỏ được và để không bao giờ cấp lại. Username, email, số điện thoại dùng lại được ngay. Không hoàn tác được.

---

## 6. Sự kiện và job nền

**Topic `account.users`** (Kafka, key = `userId`, 12 partition, **compacted**, giữ mãi bản mới nhất của mỗi user): Protobuf `pangate.v1.AccountState` trong `platform-contracts` (file mới `account.proto`). Mỗi bản ghi là **trạng thái hiện tại** của user, không phải một thay đổi, nên đọc lại từ đầu topic luôn dựng lại được toàn bộ danh sách user (chat-service mất dữ liệu, consumer chậm nhiều ngày, hay thêm consumer mới đều không mất user cũ). Không bao giờ ghi tombstone: user đã xoá giữ bản `DELETED`.

```proto
message AccountState {
  string user_id = 1;
  enum Status { STATUS_UNSPECIFIED = 0; STATUS_ACTIVE = 1; STATUS_LOCKED = 2; STATUS_DELETED = 3; }
  Status status = 2;
  int64 updated_at_ms = 3;   // để consumer bỏ qua bản cũ hơn bản đang có
}
```

Hồ sơ (tên, username) **không** đi qua topic: client đọc qua `/v1/users:batch`.

**Job** (scheduler Spring, mỗi job một advisory lock, như auth-service):

| Job | Chu kỳ | Việc |
|---|---|---|
| `EventRelay` | 200 ms | Dòng `kind = event`, tối đa 500, theo `id` → produce `account.users` (idempotent, `acks=all`). Lỗi thì dừng lượt, lượt sau gửi lại từ dòng chưa gửi đầu tiên, nên thứ tự theo `id` được giữ |
| `RevocationRelay` | 200 ms | Dòng `kind = revocation`, theo `id` → `POST /v1/revocations` của auth-service. Lỗi tạm (5xx, không trả lời, `429`) thì dừng lượt và gửi lại sau; lỗi cố định (4xx khác) thì log `error`, đếm `account.revocations.rejected` và đánh dấu đã gửi để không chặn hàng chờ. Hai relay độc lập: Kafka sập không làm chậm việc thu hồi, auth-service sập không làm chậm `account.users`. Cả hai xoá dòng đã gửi quá 7 ngày |
| `ExpireSessions` | 1 h | Phiên không dùng quá `session-idle-ttl` → `revoked_at`. Xoá phiên đã thu hồi quá 30 ngày |

---

## 7. Giới hạn và bảo mật

**Mật khẩu.** Argon2id (OWASP: m=19 MiB, t=2, p=1), như `Argon2SecretHasher` của auth-service. Mỗi lần băm tốn khoảng 50 ms CPU và 19 MiB, nên mỗi pod giới hạn số lần băm đồng thời (mặc định 2 × số core); vượt thì `503` + `Retry-After: 1` thay vì để hết bộ nhớ. Login không tồn tại vẫn băm một mật khẩu giả để thời gian trả lời như nhau.

**Rate limit** (bộ đếm trong Redis, dùng chung giữa các pod; Redis lỗi thì dùng token bucket trong bộ nhớ của pod; vượt → `429` + `Retry-After`):

| Gì | Khoá | Mặc định |
|---|---|---|
| Đăng nhập sai | login + IP | 10 lần trong 15 phút → chặn cặp đó 15 phút (`429 too_many_attempts`, kể cả mật khẩu đúng) |
| Đăng nhập sai | IP | 50 lần trong 15 phút → mỗi lần thử tiếp theo từ IP đó chờ thêm 2 s, không chặn (nhiều người dùng chung một IP qua NAT của nhà mạng); 1000 lần trong 15 phút (một đợt tấn công) → chặn IP 15 phút. Nới ngày 2026-11-17 |
| Đăng nhập sai | login (mọi IP) | 100 lần trong 15 phút → mỗi lần thử tiếp theo của login đó chờ thêm 2 s trước khi trả lời. Không chặn hẳn: username là công khai, chặn theo login một mình sẽ cho người lạ khoá được tài khoản người khác |
| Đăng ký | IP | 5 mỗi giờ |
| Đổi token | phiên | 60 mỗi giờ |
| Tra user (`/v1/users*`) | user | 600 mỗi phút |
| `/admin/v1/**` | `client_id` | 20/s |

IP lấy từ `X-Forwarded-For` do ingress đặt (cấu hình số proxy tin cậy).

**Khác:**
- Log và trace không chứa mật khẩu, token, body request; `userId` và login được hash bằng `PrincipalHasher` của starter.
- Admin API chỉ mở trên `:8081`, không nằm sau ingress.
- Không cookie, không CSRF; CORS tắt (client là app; web client là việc sau).

**Metrics:** `account.registrations{result}`, `account.logins{result}`, `account.tokens{result}`, `account.sessions.active` (gauge, ước lượng), `account.password.hash.latency`, `account.password.hash.rejected`, `account.ratelimited{what}`, `account.admin.actions{action}`, `account.outbox.pending`, `account.outbox.oldest_age_seconds`.

---

## 8. Thay đổi ở chat-service

1. **Bảng `users`** trong ScyllaDB (`user_id` → `status`: `active` | `deleted`, `updated_at`), nạp từ topic compacted `account.users` (consumer group `chat-accounts`). Trạng thái `LOCKED` coi như `active` (khoá chỉ chặn đăng nhập). Bản có `updated_at_ms` cũ hơn bản đang có bị bỏ qua.
2. **Kiểm tra user tồn tại**: tạo DM, tạo nhóm/kênh với `memberIds`, thêm thành viên với user không có trong `users` → `404 user_not_found`; user đã xoá → `404 user_deleted`. Trạng thái user được cache 10 s trong mỗi pod (đã xoá thì cache mãi). Sự kiện tạo tài khoản tới chat-service sau vài trăm ms; client mới đăng ký gặp `404` trong khoảng đó thì thử lại.
3. **Khi nhận trạng thái `DELETED`** (lần đầu): đặt `users.status = deleted`; với mọi hội thoại trong `user_convs` của user đó:
   - nhóm và kênh: rời hội thoại như khi tự rời (thu hồi subscription kênh như CH1). Nếu người đó là owner: chuyển owner cho admin vào sớm nhất, không có thì cho thành viên vào sớm nhất; hội thoại không còn ai thì giữ nguyên, không còn ai gửi được;
   - DM: giữ nguyên; gửi tin vào DM có người kia đã xoá → `ChatRejected` / `404 user_deleted`.
   Xử lý idempotent: chạy lại cùng sự kiện không đổi gì thêm.
4. Tin cũ giữ `senderId`; client hiện "Tài khoản đã xoá" khi `/v1/users:batch` trả `deleted: true`.

---

## 9. Thay đổi ngoài `account-service` và `chat-service`

| Module | Thay đổi |
|---|---|
| `platform-contracts` | `account.proto` (`AccountState`), topic `account.users` trong testkit; `buf breaking` xanh |
| `auth-service` | Scope `accounts:admin`; seed thêm client `chat-admin` (tenant `chat`, scope `accounts:admin`) |
| `platform-devenv` | Service `account` (profile `app`, cổng `8088:8080`, `8089:8081`), schema `account` trên container `postgres` sẵn có, topic `account.users` (`cleanup.policy=compact`) trong `kafka-init` |
| `gateway-devtools` | Lệnh `account register` / `account login` / `account token`; `smoke` thêm bước tài khoản (§10) |
| `Makefile` | `images` build `account-service`; `smoke` chạy bước tài khoản |

---

## 10. Kiểm thử

| Mức | Nội dung |
|---|---|
| Unit (domain, use case, không Spring) | Quy tắc §4.1; chuẩn hoá email/số điện thoại; đăng nhập bằng cả ba loại login; lỗi đăng nhập không lộ tài khoản tồn tại; must-change-password; khoá / xoá chặn đổi token; giới hạn đăng nhập sai; giới hạn băm đồng thời |
| IT (Testcontainers PostgreSQL, Redis, Kafka; auth-service giả bằng WireMock hoặc auth-service thật) | Đăng ký đua nhau cùng username / email: đúng một thắng; xoá rồi đăng ký lại cùng username / email; mỗi relay gửi bù đúng thứ tự khi Kafka / auth-service quay lại, và relay này sập không chặn relay kia; lỗi cố định của auth-service không chặn hàng chờ; refresh token đã thu hồi bị từ chối; migration chạy lại không lỗi |
| IT chat-service | `DELETED` → rời nhóm, chuyển owner, DM từ chối; user chưa có → `user_not_found`; chạy lại bản ghi không đổi gì; đọc lại topic từ đầu dựng lại đủ bảng `users` |
| E2E (devenv, `make smoke`) | Đăng ký A, B → đăng nhập → đổi token → HELLO vào gateway → A gửi DM cho B qua chat-service → B nhận; B đăng xuất thiết bị → 4003 < 10 s; admin khoá A → 4003, đăng nhập bị từ chối; mở khoá → A đăng nhập lại và HELLO được ngay |

---

## 11. ADR mới

| # | Nội dung |
|---|---|
| 0033 | Tài khoản app chat là service riêng trên PostgreSQL, không nằm trong chat-service hay auth-service |
| 0034 | Refresh token mỗi thiết bị đổi lấy connect token 10 phút; connect token là bearer cho gateway lẫn HTTP API của chat-service và account-service |
| 0035 | chat-service biết user qua topic compacted `account.users` mang trạng thái hiện tại (bảng `users`), không gọi account-service đồng bộ |
| 0036 | account-service thu hồi theo `device`, chỉ xoá tài khoản mới thu hồi `principal`, vì gateway chặn thu hồi suốt TTL bất kể `iat` |

---

## 12. Để lại cho người hiện thực

- Tách bộ kiểm connect token (JWKS + `gw.control`) của chat-service thành thư viện dùng chung, hay chép sang account-service.
- Test với auth-service thật hay giả trong IT của account-service.
- Cách đo `account.sessions.active`.
- Danh sách username cấm cụ thể.
