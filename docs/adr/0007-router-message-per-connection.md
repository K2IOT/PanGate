# ADR 0007: Router gửi `MESSAGE` theo từng kết nối (`conn_id` bắt buộc)

## Trạng thái

Accepted, 2026-11-05.

## Bối cảnh

Một principal có thể có nhiều thiết bị và nhiều kết nối, có thể ở nhiều node. Nếu router gửi theo principal, gateway phải tự fan-out và ack cho từng kết nối sẽ bị gộp.

## Quyết định

Mỗi `PushItem` kind `MESSAGE` mang `conn_id` bắt buộc; router gửi một item cho mỗi kết nối và nhận `DeliveryAck` theo từng `delivery_id`. Broadcast theo topic dùng kind `BROADCAST`.

## Hệ quả

- Gateway giao đúng một kết nối cho mỗi item, ack rõ ràng.
- Router phải đọc registry ở mức kết nối; contract cần team router xác nhận (spec §14, câu hỏi mở 2).

## Nguồn

Spec §6.3.
