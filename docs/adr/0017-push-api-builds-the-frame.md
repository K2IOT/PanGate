# ADR 0017: Push API dựng sẵn `Frame`; router không đọc payload

## Trạng thái

Accepted, 2026-11-10.

## Bối cảnh

Gateway nhận `PushItem.frame`, tức một `Frame` PUSH đã dựng sẵn. Cần quyết định nơi dựng frame: Push API hay router.

## Quyết định

- `PushRecord.frame` mang một `Frame` hoàn chỉnh: `msg_id`, `class`, `payload` và `headers`, cộng `seq`/`seq_epoch` (MESSAGE) hoặc `topic`/`topic_seq` (BROADCAST).
- Push API dựng frame này. Router chỉ tra registry rồi bọc nó vào `PushItem`.
- Inbox (Phase 2) lưu nguyên frame đó.

## Hệ quả

- Push API vẫn là nơi duy nhất quyết định nội dung đi xuống, đúng với bất biến "đường xuống duy nhất".
- Router không phụ thuộc định dạng payload, và frame chỉ được encode một lần.
- Record nặng hơn không đáng kể, vì frame chứa chính payload.

## Nguồn

Spec push contracts §1.1 (C4); plan C1 Task 3.
