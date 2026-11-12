# ADR 0031: Fan-out theo thành viên cho 1-1 và nhóm; BROADCAST theo topic cho kênh, subscribe do server quyết

## Trạng thái

Accepted, 2026-11-12.

## Bối cảnh

Push API có hai cách giao tin: MESSAGE tới một principal (RELIABLE, có ACK, người offline đi vào `cN.push.offline`) và BROADCAST tới một topic (EPHEMERAL, chỉ phiên đang subscribe nhận). Nhóm nhỏ cần mỗi thành viên nhận đủ tin. Kênh có thể có hàng triệu người theo dõi, nên không thể gửi từng người.

## Quyết định

- 1-1 và nhóm (tối đa 500 người): mỗi thành viên một `PushRequest` loại MESSAGE, RELIABLE, `principal_id=chat:<userId>`, `headers.type=chat.message`, không có `idempotency_key`. Cả thiết bị của người gửi cũng nhận. Sau khi gửi xong, fan-out cập nhật `user_convs.last_seq` của từng thành viên.
- Kênh: một BROADCAST EPHEMERAL tới topic `chat:chan:<convId>`. `user_convs` của người theo dõi không đổi; họ đồng bộ bằng `/v1/sync`.
- Mở và đóng kênh: client gửi `chat.channel.open` / `chat.channel.close` qua WebSocket. chat-service kiểm quyền (kênh công khai, hoặc là thành viên của kênh riêng), rồi gửi CONTROL `subscribe` / `unsubscribe` tới `chat:<userId>` qua Push API. Lý do: gateway chỉ kiểm tra tenant của topic, nên quyền phải nằm ở chat-service.
- Lỗi tạm (không có trả lời, `UNAVAILABLE`, `RATE_LIMITED`) thì chỉ gửi lại các request đó, backoff từ 100 ms tới 5 s, không commit. Sau 30 s thì quay partition về sự kiện đó. Lỗi cố định thì đếm và bỏ qua.

## Hệ quả

- Một tin nhóm 500 người tốn 500 push, gửi song song trên một `SendStream`.
- Không đặt idempotency key, vì 500 key Redis mỗi tin là quá đắt. Bù lại, khi gửi lại thì client có thể nhận trùng; client dedup theo `ChatMessage.msg_id`.
- CONTROL subscribe áp cho mọi kết nối của user. Client mở lại kênh sau khi nối lại.
- Người theo dõi kênh không nhận được tin khi offline (CH4 sẽ làm push mobile).

## Nguồn

Spec chat-core §0 (D2), §6.
