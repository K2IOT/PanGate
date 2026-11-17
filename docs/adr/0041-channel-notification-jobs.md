# ADR 0041: Thông báo kênh: một job mỗi kênh từ `chat.events`, bài sau tiếp con trỏ của bài trước

## Trạng thái

Accepted, 2026-11-17.

## Bối cảnh

Bài kênh đi theo kiểu broadcast nên không bao giờ vào `push.offline`. Một kênh có thể có hàng triệu người theo dõi: mỗi bài là chừng ấy thông báo, và hai bài liên tiếp không được thành hai lượt cho cả triệu người.

## Quyết định

- `ChannelNotifierLoop` (group `chat-channel-notify`) đọc `chat.events` (bài đã lưu bền, key = hội thoại), lấy bài kênh, và duyệt `members` theo trang 1 000 người (thứ tự `user_id`), bỏ người gửi, người đang online, người đã tắt kênh (trừ khi được nhắc tên).
- Một job sống mỗi kênh. Bài mới tới khi job cũ chưa xong thì **tiếp con trỏ** của job cũ: mỗi người theo dõi nhận một thông báo cho một loạt bài; `collapse_id` = kênh nên máy chỉ giữ thông báo mới nhất.
- Sau mỗi trang lưu con trỏ vào `channel_notify_progress` (TTL 1 ngày); đọc lại sau restart hay rebalance thì đi tiếp từ đó; chỉ trang cuối có thể gửi lại, notify-service dedup theo `request_id`. Offset commit tới job chưa xong cũ nhất của partition.
- Mỗi pod tối đa 5 000 yêu cầu/s cho kênh (`chat.notifications.channel-rate`); bài cũ hơn 1 giờ bỏ; thông báo kênh không đổi badge.

## Hệ quả

- Người theo dõi được báo trước khi bài mới tới thì thấy nội dung bài cũ cho tới khi mở app.
- Đo trên máy dev 4 vCPU: kênh 100 000 người theo dõi offline, 100 000 yêu cầu được Kafka ack trong 20,0 s (5 002/s, đúng giới hạn tốc độ).
- Không cần topic job riêng; notifier đọc mọi sự kiện chat và lọc bài kênh (rẻ).

## Nguồn

Spec CH4 §4.
