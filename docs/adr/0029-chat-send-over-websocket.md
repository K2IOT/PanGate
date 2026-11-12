# ADR 0029: Gửi tin qua WebSocket là đường chính, HTTP là đường phụ; không sửa gateway

## Trạng thái

Accepted, 2026-11-12.

## Bối cảnh

Client chat đã giữ một WebSocket tới gateway để nhận tin. Gửi tin cũng có thể đi qua kết nối đó: gateway đã có `UPSTREAM` → `cN.gw.upstream.<type>` kèm `UPSTREAM_ACK` (spec gateway §5.5). Cách khác là client gọi HTTP của chat-service cho mỗi tin. Cách đó tốn thêm một kết nối và thêm handshake TLS trên mạng di động, nhưng dễ dùng cho bot và script.

## Quyết định

- Đường chính: `UPSTREAM` với `headers.type=cmd`, `headers.op=chat.send` (cùng `chat.channel.open` / `chat.channel.close`), payload là protobuf `ChatSend` / `ChannelOpen` / `ChannelClose`. chat-service đọc `c1.gw.upstream.cmd` theo consumer group `chat-ingest` và chỉ lấy record của tenant `chat`.
- Đường phụ: `POST /v1/conversations/{id}/messages` với `Idempotency-Key` (chính là `clientMsgId`). Pod chờ kết quả tối đa 3 s qua Redis: có thì trả `201`, quá hạn hoặc không có Redis thì trả `202`.
- Không đổi gì ở gateway: `op` chỉ là một header thường của frame.
- Lệnh bị từ chối trên WebSocket được báo lại cho các thiết bị của người gửi bằng `chat.rejected` (MESSAGE EPHEMERAL qua Push API).

## Hệ quả

- `UPSTREAM_ACK` chỉ có nghĩa "server đã nhận", chưa phải "đã vào hội thoại". Client biết tin đã vào hội thoại khi nhận `ChatMessage` có cùng `clientMsgId`.
- Hai đường dùng chung bộ kiểm tra (`Ingest`) và cùng ghi `chat.commands`, nên gửi lại cùng một `clientMsgId` qua cả hai đường vẫn chỉ ra một tin.
- Topic upstream chung cho mọi tenant: chat-service phải bỏ qua record của tenant khác.

## Nguồn

Spec chat-core §2, §4.1–§4.2.
