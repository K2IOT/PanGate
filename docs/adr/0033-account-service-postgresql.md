# ADR 0033: Tài khoản app chat là service riêng trên PostgreSQL

## Trạng thái

Accepted, 2026-11-16.

## Bối cảnh

App chat cần người dùng thật: đăng ký, đăng nhập bằng email / số điện thoại / username + mật khẩu, phiên theo thiết bị, hồ sơ, và thao tác của vận hành. Username, email và số điện thoại phải duy nhất cùng lúc. auth-service là dịch vụ cấp token của nền tảng, đa tenant, không biết user cuối; chat-service giữ hội thoại trên ScyllaDB.

## Quyết định

- Module mới `account-service`, bố cục hexagonal như auth-service, dữ liệu trong schema `account` của PostgreSQL (Flyway `classpath:db/account`).
- Một transaction tạo tài khoản, định danh, phiên đầu tiên và bản ghi outbox; ràng buộc unique của PostgreSQL giữ ba khoá duy nhất.
- account-service là "backend của tenant `chat`": nó xin auth-service connect token cho `chat:<userId>`.

## Hệ quả

- Không cần ghép nhiều LWT của ScyllaDB cho ba khoá duy nhất; vài chục triệu dòng tài khoản nằm trong sức PostgreSQL.
- Thêm một service và một schema phải vận hành.
- Không trộn mật khẩu và phiên vào lõi chat hay vào auth-service.

## Nguồn

Spec account-service §1.
