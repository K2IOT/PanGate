# ADR 0023: Idempotency giữ sẵn `msgId` và seq cho mỗi key

## Trạng thái

Accepted, 2026-11-10.

## Bối cảnh

Contracts §3.3 lưu `PushResponse` đầu tiên cho mỗi `idempotency_key` trong 1 giờ. Nếu produce lỗi hoặc pod chết sau khi produce mà trước khi lưu response, lần retry không được sinh `msgId` hay seq mới, vì client có thể nhận hai bản.

## Quyết định

- Request đầu chiếm `idem:{t}:<key>` bằng `pending|msgId|seq|epoch|topicSeq` (`SET NX`, 30 s), và sao giá trị đó vào `idem:{t}:<key>:last` (1 giờ).
- Thành công: ghi `done|<PushResponse>`; request sau nhận lại nó với `duplicate=true`.
- Produce lỗi: xoá `pending` (chỉ khi còn là của mình); `:last` giữ nguyên.
- Lần chiếm sau đọc `:last` trước khi cấp mới, nên retry produce lại đúng `msgId` và seq; phía dưới dedup theo `msg_id`.
- Gặp `pending` của request khác: chờ tối đa 2 s rồi trả `503`.

## Hệ quả

- Retry an toàn sau `503` hay sau crash; không có lỗ seq do retry.
- Hai key Redis cho mỗi request có key; không ảnh hưởng request không có key.

## Nguồn

Spec push-api §2.1; plan P1 Task 4.
