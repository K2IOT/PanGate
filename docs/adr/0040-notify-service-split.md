# ADR 0040: notify-service của nền tảng giữ token và gửi APNs/FCM; app quyết định nói gì

## Trạng thái

Accepted, 2026-11-17.

## Bối cảnh

App chat cần thông báo đẩy khi người nhận không có thiết bị nào nhận được tin (CH4). Có ba chỗ đặt phần này: gói hết vào chat-service, một service nền tảng làm cả phần chat, hoặc tách đôi. Spec contracts đã vẽ "Offline Delivery" là một thành phần của nền tảng.

## Quyết định

- **notify-service** (nền tảng, không biết gì về chat): API `PUT|DELETE /v1/devices/self/push` theo connect token (tenant, principal, device lấy từ token); bảng `notify.push_tokens` (một token một thiết bị); gỡ token theo `gw.control` (`Revoke{device}`, `Revoke{principal}`) và khi Apple/Google báo token chết; đọc `cN.notify.requests` và gửi APNs (HTTP/2, JWT ES256) / FCM HTTP v1 (OAuth2 service account) bằng khoá cấu hình theo tenant; dedup theo `request_id` trên Redis.
- **chat-service** soạn nội dung: đọc `cN.push.offline` (DM, nhóm) và `chat.events` (kênh), kiểm tắt thông báo và ẩn xem trước, tính badge, produce `NotifyRequest`.
- Giữa hai bên là Kafka (`cN.notify.requests`, key = người nhận), không gọi đồng bộ: notify ngừng thì yêu cầu chờ, chat không chậm theo.

## Hệ quả

- Tenant khác dùng lại notify-service chỉ bằng cách produce `NotifyRequest` và cấu hình khoá Apple/Google của app mình.
- Một yêu cầu gửi trùng (router ghi `OfflineRecord` hai lần, loop tua lại) bị chặn ở notify theo `request_id`; một crash giữa lúc đánh dấu dedup và lúc gửi làm mất đúng thông báo đó (chấp nhận).
- Devenv và test dùng APNs/FCM giả của gateway-devtools; khoá thật cấu hình khi có app.

## Nguồn

Spec CH4 §1, §2, §5, §9.
