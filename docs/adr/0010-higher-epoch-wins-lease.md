# ADR 0010: Lease "epoch lớn hơn thắng" để restart nhanh không bị fence

## Trạng thái

Accepted, 2026-11-05.

## Bối cảnh

Với quy tắc "lease còn sống thì chặn", process vừa restart phải chờ lease cũ (15 s) hết hạn mới chạy được.

## Quyết định

Lua lease: nếu `node:{nodeId}` đang giữ epoch lớn hơn epoch của process thì trả `-1` (process tự fence: draining, log ERROR, `System.exit(2)`); ngược lại ghi epoch mới với TTL. Process restart luôn có epoch lớn hơn nên chiếm lease ngay.

## Hệ quả

- Restart nhanh, không chờ 15 s.
- Hai process cấu hình nhầm cùng `nodeId`: process cũ hơn tự fence.
- `ZADD nodes:live` chạy riêng sau script (khác slot trên Redis Cluster).

## Nguồn

Spec §6.1, §14.8.
