# ADR 0015: Thu hồi qua transactional outbox lên `gw.control`

## Trạng thái

Accepted, 2026-11-10.

## Bối cảnh

Một lệnh thu hồi đã trả `202` thì không được mất, kể cả khi Kafka sập hoặc Auth khởi động lại ngay sau đó (§3.3 tài liệu gốc). Gateway đọc `gw.control` (compacted) từ đầu topic mỗi lần boot.

## Quyết định

- Dòng `auth.revocations` và dòng `auth.outbox` được ghi trong một transaction. Thu hồi lại một khoá còn hiệu lực sẽ gia hạn chính dòng đó và ghi một dòng outbox mới.
- `OutboxRelay`, dưới lock `auth.outbox-relay`, gửi lần lượt theo `id` bằng producer idempotent `acks=all`. Relay chờ ack từng record và dừng ở lỗi đầu tiên, nên thứ tự được giữ.
- Khi thu hồi hết hạn (24 h, bằng TTL dài nhất của resume ticket), Auth ghi tombstone cùng khoá và xoá dòng.
- Producer được tạo lười, nên Auth khởi động và báo ready mà không cần Kafka.

## Hệ quả

- Kafka sập: `POST /v1/revocations` vẫn trả `202`. Outbox dồn lại (metric `auth.outbox.oldest_age_seconds`) và được gửi bù đúng thứ tự khi Kafka trở lại, kể cả sau khi Auth restart.
- Có thể gửi trùng (at-least-once). `gw.control` là topic compacted và gateway áp dụng idempotent theo khoá, nên trùng không gây hại.
- Bảng outbox có thêm cột `tenant_id` (migration V2) cho header `tenant-id`.

## Nguồn

Spec auth §4.4, §5, §6 (D6); plan A1 Task 9; `RevocationIT`.
