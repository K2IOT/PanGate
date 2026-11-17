# ADR 0038: Online theo đăng ký xem của client, luật chung hội thoại, ân hạn 5 s

## Trạng thái

Accepted, 2026-11-17.

## Bối cảnh

Báo online cho mọi người chung hội thoại khi trạng thái đổi tốn khoảng 5 000 push mỗi lần một người ở 10 nhóm 500 người chập chờn mạng. Người lạ không được dò được ai đang online.

## Quyết định

- chat-service đọc `c1.gw.lifecycle` (group `chat-presence`, từ đầu topic) vào Redis: tập kết nối mỗi user, chỉ mục theo node để xử lý `NODE_DOWN`, dedup theo `event_id`, TTL 24 h.
- Tập về rỗng thì chờ ân hạn 5 s; vẫn rỗng mới là offline, `last_seen` = lúc ngắt; nối lại trong ân hạn không phát gì.
- Client gửi `chat.presence.watch` với tối đa 200 cặp `(userId, conversationId)` đang hiện trên màn hình, gửi lại mỗi 4 phút; server giữ 5 phút. Mỗi mục chỉ được giữ khi cả hai là thành viên của hội thoại đó (không phải kênh); mục sai bỏ lặng lẽ.
- Thay đổi chỉ đẩy tới thiết bị đang xem; ẩn online là có qua có lại.

## Hệ quả

- Redis sập: mọi người hiện offline, watch không lưu được; tự phục hồi khi Redis về và client watch lại. Người đang nối sẵn hiện offline tới lần nối lại kế tiếp (chấp nhận ở CH3).
- Có thể tách thành service presence của nền tảng (scope `presence:read`) khi có app thứ hai.

## Nguồn

Spec CH3 §6, §8, §9, §11.
