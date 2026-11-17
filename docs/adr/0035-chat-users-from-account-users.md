# ADR 0035: chat-service biết user qua topic compacted `account.users`

## Trạng thái

Accepted, 2026-11-16.

## Bối cảnh

chat-service phải từ chối đưa user không tồn tại hoặc đã xoá vào hội thoại, và phải cho user bị xoá rời nhóm. Gọi account-service đồng bộ ở mỗi thao tác sẽ ghép hai service lại với nhau.

## Quyết định

- account-service ghi `AccountState{user_id, status, updated_at_ms}` (trạng thái hiện tại, không phải thay đổi) lên topic compacted `account.users`, key = userId, không bao giờ tombstone; qua outbox.
- chat-service đọc từ đầu (group `chat-accounts`) vào bảng `users` của ScyllaDB; bản cũ hơn bản đang có bị bỏ qua. Lần đầu một user thành `DELETED`, nó rời mọi nhóm và kênh (owner chuyển cho admin vào sớm nhất, không có thì member sớm nhất) trước khi trạng thái được ghi.

## Hệ quả

- Đọc lại topic từ đầu luôn dựng lại đủ danh sách user (mất dữ liệu, consumer chậm nhiều ngày, consumer mới).
- User mới đăng ký chỉ vào được hội thoại sau vài trăm ms (`404 user_not_found` trong khoảng đó).

## Nguồn

Spec account-service §6, §8.
