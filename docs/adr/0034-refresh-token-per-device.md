# ADR 0034: Refresh token mỗi thiết bị đổi lấy connect token 10 phút

## Trạng thái

Accepted, 2026-11-16.

## Bối cảnh

Client cần một bằng chứng đăng nhập lâu dài cho mỗi thiết bị, và một token ngắn hạn để vào gateway và gọi HTTP API. auth-service đã cấp connect token (`aud=pangate-gw`, tối đa 10 phút) và thu hồi theo jti / thiết bị / principal.

## Quyết định

- Đăng nhập tạo một phiên (thiết bị) với `deviceId` (ULID) và refresh token `rt_…` 32 byte ngẫu nhiên; chỉ lưu SHA-256. Không xoay vòng; hết hạn sau 180 ngày không dùng.
- `POST /v1/sessions/token` đổi refresh token lấy connect token 600 s cho `chat:<userId>` / `deviceId`, do account-service xin auth-service.
- Connect token là bearer cho gateway, HTTP API của chat-service và HTTP API của account-service (account-service còn kiểm phiên còn sống, cache 10 s).

## Hệ quả

- Không có bộ khoá ký hay loại token mới; một lệnh thu hồi có tác dụng ở mọi nơi.
- Lộ refresh token cho phép lấy connect token tới khi thiết bị bị đăng xuất.

## Nguồn

Spec account-service §2.
