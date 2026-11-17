# ADR 0039: Hợp đồng đồng bộ phía client

## Trạng thái

Accepted, 2026-11-17.

## Bối cảnh

Push RELIABLE có thể tới trùng, và client rớt mạng thì lỡ tin. Server phải cho client biết đúng vị trí của mình trong từng hội thoại và bù đúng phần thiếu.

## Quyết định

- Client giữ `syncedSeq` mỗi hội thoại: mọi tin `≤ syncedSeq` đã có. PUSH `convSeq = syncedSeq + 1` thì thêm; `≤ syncedSeq` là trùng (dedup theo `msg_id`); lớn hơn là có lỗ → `POST /v1/sync {id, lastSeq: syncedSeq}` rồi ghép.
- `convSeq` liên tục không lỗ (CH1), nên lỗ ở client luôn là tin bị thiếu.
- Sau khi nối lại: mở lại kênh, watch lại, `GET /v1/state?since=<now lần trước>` (hội thoại có thay đổi, kèm `lastSeq`, `readSeq`, `unread`, mốc của người kia trong DM), rồi `/v1/sync` cho hội thoại có `lastSeq > syncedSeq`.
- `user_convs.state_at` đánh dấu mỗi thay đổi (tin mới, mốc của tôi, mốc của người kia trong DM).

## Hệ quả

- Router cache tra phiên 2 s (kể cả kết quả rỗng): tin gửi ngay sau khi một user vừa nối có thể vào hàng offline; hợp đồng trên bù nó ở lần đồng bộ kế tiếp.

## Nguồn

Spec CH3 §7.
