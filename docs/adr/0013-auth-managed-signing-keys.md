# ADR 0013: Auth tự quản lý khoá ES256 trong PostgreSQL

## Trạng thái

Accepted, 2026-11-10.

## Bối cảnh

Gateway kiểm tra connect JWT offline qua JWKS (§3.2 tài liệu gốc, spec gateway §4.9). Auth phải tự xoay khoá ký 30 ngày một lần, với 7 ngày chồng lấn, mà không làm gãy token đang lưu hành và không cần Vault trong devenv. Auth cũng chạy nhiều pod, nên phải tránh việc hai pod cùng xoay khoá.

## Quyết định

- Auth tự sinh key P-256. Private key được mã hoá AES-256-GCM bằng KEK 32 byte đọc qua `SecretSource` (`auth.kek.<id>`), với AAD là `kid`, rồi lưu ở `auth.signing_keys`. `kid` là thumbprint RFC 7638.
- Vòng đời `pending → active → retired`:
  - key mới được publish trước 1 giờ rồi mới dùng để ký;
  - key cũ giữ trên JWKS thêm 7 ngày sau khi retire.
- Mỗi lượt xoay chạy trong một transaction có giữ `pg_try_advisory_xact_lock('auth.rotate-keys')`. Unique index `one_active_key` là lớp bảo vệ thứ hai.
- JWKS và khoá ký được phục vụ từ bộ nhớ (`KeyRing`), nạp lại sau mỗi lần xoay và theo chu kỳ `cache-refresh` (60 s).

## Hệ quả

- PostgreSQL sập không ảnh hưởng JWKS hay việc kiểm tra token. Chỉ việc cấp token mới trả 503.
- Mất KEK thì không giải mã được key cũ. Key `active` không giải mã được sẽ được thay ngay. KEK thiếu khi khởi động thì fail fast qua `KekFailureAnalyzer`.
- Pod không thực hiện lượt xoay sẽ thấy key `pending` mới trong vòng `cache-refresh`, sớm hơn nhiều so với mốc 1 giờ trước khi key đó được dùng.
- Timestamp được cắt về micro giây trước khi ghi, vì PostgreSQL làm tròn và có thể đẩy `activate_at` lên sau thời điểm mong muốn.

## Nguồn

Spec auth §3, §6 (D4); plan A1 Task 5; `KeyRotationIT`.
