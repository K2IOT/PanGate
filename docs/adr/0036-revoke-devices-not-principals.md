# ADR 0036: account-service thu hồi theo thiết bị; chỉ xoá tài khoản mới thu hồi principal

## Trạng thái

Accepted, 2026-11-16.

## Bối cảnh

Gateway chặn một lệnh thu hồi `principal` hoặc `device` suốt TTL 24 h với mọi token, kể cả token cấp sau đó (`RevocationList.isRevoked` không xét `iat`). Nếu đổi mật khẩu, khoá hay "đăng xuất mọi thiết bị" thu hồi principal, user sẽ không vào lại gateway trong 24 h, kể cả sau khi mở khoá.

## Quyết định

- Đăng xuất, đổi mật khẩu (trừ thiết bị đang dùng), đặt lại mật khẩu và khoá: đánh dấu từng phiên thu hồi và gửi một thu hồi `device` cho mỗi phiên. `deviceId` không bao giờ dùng lại, nên thu hồi chỉ chặn đúng phiên cũ; đăng nhập lại tạo `deviceId` mới.
- Chỉ xoá tài khoản thu hồi `principal` (user đó không bao giờ đăng nhập lại).
- Lệnh thu hồi đi qua outbox của account-service, relay riêng với relay sự kiện; lỗi cố định của auth-service được ghi log và bỏ qua.

## Hệ quả

- Mở khoá và đăng nhập lại có tác dụng ngay.
- Khoá một user có nhiều thiết bị gửi nhiều lệnh thu hồi (auth-service giới hạn 100/s mỗi client; outbox gửi dần).

## Nguồn

Spec account-service §2, §5.
