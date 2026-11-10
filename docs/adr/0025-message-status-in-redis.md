# ADR 0025: Trạng thái message trong Redis, TTL theo message

## Trạng thái

Accepted, 2026-11-10.

## Bối cảnh

Tenant backend cần biết một push đã được giao hay đã offline, mà không thêm cơ sở dữ liệu cho đường accept.

## Quyết định

- Khi accept, ghi hash `msg:{t}:<msgId>` (`state=accepted`, kind, class, receipt, target, thời điểm), TTL = `expires_at` − now + 1 giờ.
- Một consumer group đọc `cN.push.offline` và `cN.push.delivered` và cập nhật bằng script: chỉ khi key tồn tại; `delivered` thắng `offline`.
- `GET /v1/messages/{msgId}` đọc theo tenant của token.
- Router chỉ ghi `push.delivered` khi `receipt=true`, nên message không receipt chỉ có `accepted` hoặc `offline`; BROADCAST chỉ có `accepted`.

## Hệ quả

- Không thêm hạ tầng; trạng thái hết hạn cùng message.
- Ghi trạng thái là best effort; Redis lỗi không làm request thất bại.

## Nguồn

Spec push-api §4.2; plan P1 Tasks 7, 9.
