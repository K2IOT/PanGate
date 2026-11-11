# ADR 0026: Rate limit push là token bucket trên mỗi pod

## Trạng thái

Accepted, 2026-11-10.

## Bối cảnh

Push API cần giới hạn tần suất theo tenant mà không thêm một round-trip Redis cho mỗi request.

## Quyết định

- Token bucket trong bộ nhớ theo tenant × kind: MESSAGE dùng `push_rate` / `push_burst`, BROADCAST dùng `broadcast_rate`; CONTROL không bị giới hạn.
- Mặc định từ cấu hình (1 000/s, burst 2 000, broadcast 10/s); ghi đè qua `TenantLimits.push_rate = 8`, `push_burst = 9`, `broadcast_rate = 10` trên `gw.control` (additive; gateway bỏ qua).
- Vượt giới hạn → `429` với `Retry-After` tính từ bucket.
- `CODE_QUOTA_EXCEEDED` chưa dùng.

## Hệ quả

- Giới hạn thực tế = giới hạn × số pod; người vận hành phải tính khi đặt.
- Không phụ thuộc Redis cho đường accept.

## Nguồn

Spec push-api §4.1, §11.1; plan P1 Tasks 1, 5.
