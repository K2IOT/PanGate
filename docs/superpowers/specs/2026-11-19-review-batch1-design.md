# Đợt 1 sau review toàn bộ CH1–CH4: nghiệp vụ và quyền riêng tư

- **Trạng thái:** Đã duyệt hướng (2026-11-19). Nguồn: review toàn bộ chat sau khi merge K2IOT/PanGate#8.
- **Phạm vi:** 8 lỗi người dùng gặp ngay. Chống lạm dụng, vận hành (đợt 2) và hiệu năng (đợt 3) làm riêng.

## Quyết định của chủ dự án

| # | Câu hỏi | Chọn |
|---|---|---|
| Q1 | Chủ rời nhóm/kênh | Quyền chủ tự chuyển (admin vào sớm nhất, không có thì thành viên vào sớm nhất), như khi xoá tài khoản. Thêm API: chủ cấp/thu quyền admin, chủ chuyển quyền chủ |
| Q2 | Người online chưa mở kênh | chat-service ghi lại kênh mỗi người đang mở; thông báo kênh chỉ bỏ qua người online **và** đang mở kênh đó |
| Q3 | Thêm người không cần đồng ý | Cài đặt quyền riêng tư `whoCanAdd`: `everyone` (mặc định) hoặc `nobody`; tối đa 200 người mỗi lần tạo |

## Các sửa

**F1. Vào nhóm không kéo theo lịch sử chưa đọc.** Khi một người được thêm vào nhóm/kênh đã có tin (thêm, tự vào kênh công khai, vào lại), dòng `user_convs` của họ nhận `last_seq`, `read_seq`, `delivered_seq` = `last_seq` hiện tại của hội thoại, ghi `USING TIMESTAMP <last_seq>` như mọi mốc. `conv_marks` không đổi: họ không được tính là đã xem các tin trước khi vào.

**F2. Không bao giờ mất người quản lý.**
- Chủ tự rời (`DELETE .../members/<chính mình>`): quyền chủ chuyển trước như `leaveDeletedAccount`, rồi mới rời. Người cuối cùng rời thì nhóm không còn ai, không cần chủ.
- `PUT /v1/conversations/{id}/members/{userId}/role` `{"role":"admin"|"member"}`: chỉ chủ; người đích phải là thành viên và không phải chủ.
- `POST /v1/conversations/{id}/owner` `{"userId":...}`: chỉ chủ; người đích phải là thành viên; người đích thành chủ, chủ cũ thành admin.
- DM: 400 như các thao tác thành viên khác.

**F3. `hideRead` không lộ qua HTTP.**
- `conv_marks` thêm cột `shown_delivered_seq`: chỉ tăng theo mốc đến từ `chat.delivered` thật (`deliveredOnly`), ghi `USING TIMESTAMP` như các mốc khác.
- Khi một cặp không được thấy nhau đọc (`PrivacyRules.mayShowRead` sai), `peer.deliveredSeq` và `/receipts` trả `shown_delivered_seq` thay vì `delivered_seq`.
- Worker chỉ đóng dấu `state_at` cho người kia của DM khi họ thấy được thay đổi: mốc đọc thấy được, hoặc có `chat.delivered` thật.

**F4. Badge tính tin đang báo.** `Badge.total(user, convId, convSeq)`: hội thoại của tin đang báo tính `last_seq` ít nhất bằng `convSeq` của tin đó (dòng inbox có thể chưa được fan-out nâng).

**F5. Sự kiện ngắt kết nối không bị mất khi Redis lỗi.**
- Sự kiện chỉ được đánh dấu đã xử lý (`presev:`) sau khi áp dụng xong. Áp dụng lỗi thì gỡ dấu.
- `LifecycleListener` không bỏ qua bản ghi lỗi nữa: quay lại bản ghi đó (seek) trên mọi partition của batch, chờ (200 ms → 5 s), rồi thử lại. Thao tác presence là idempotent nên lặp lại an toàn. Khi Redis hỏng, presence vốn không chạy; khi Redis về, topic được đọc tiếp theo thứ tự.
- Sửa spec CH3 §9: thay "bỏ qua" bằng "thử lại".

**F6. Kênh đang mở.**
- `chat.channel.open` thêm `convId` vào `chanopen:<userId>` (Redis set, TTL 24 h, gia hạn mỗi lần mở); `close` và thu hồi sau khi rời thì gỡ; khi người đó chuyển sang offline (hết grace) thì xoá cả tập.
- `ChannelNotifierLoop` coi một người đã nhận bài khi họ online **và** `convId` có trong `chanopen:<userId>` (SISMEMBER gửi một lượt, pipelined như `onlineOf`). Người online chưa mở kênh nhận thông báo như người offline.
- Thông báo chỉ là xấp xỉ "đã nhận": một thiết bị mới nối sau khi mở có thể chưa nhận bài kênh cho tới khi app mở lại kênh.

**F7. Ai được thêm tôi.**
- `user_settings.who_can_add` (`everyone` | `nobody`, null = `everyone`). `PUT /v1/me/privacy` nhận `whoCanAdd`; `GET` trả nó.
- `addMember` vào người `nobody`: 403 `add_refused`. Tạo nhóm/kênh: người `nobody` không được thêm, phản hồi 201 có `notAdded: [...]`. Kênh công khai họ vẫn tự vào được.
- Đọc cài đặt từ store (không cache), như `hidePreview` ở DM.
- `memberIds` khi tạo tối đa 200 (400 nếu quá); giới hạn nhóm 500 giữ nguyên.

**F8. Budget connect token không bị trừ khi auth-service lỗi.** `ExchangeToken.exchange`: auth-service không trả lời hoặc từ chối thì trả lại lượt đã đếm (`counter.decrement`).

## Ngoài phạm vi đợt này
Giới hạn tần suất thêm người, lời mời có chấp nhận, `whoCanAdd = contacts` (chưa có danh bạ), Android `collapse_key` (cần thử FCM thật), mọi điểm nghẽn (đợt 3).

## Sau review của đợt 1

Reviewer độc lập đọc toàn bộ thay đổi của đợt 1. Đã sửa (mỗi sửa có test fail trước):
- **I1 (F2):** quyền chủ nằm ở `conversations.owner_id` (V8), chỉ đổi bằng LWT `IF owner_id = <chủ hiện tại>`; hai lần chuyển chen nhau hay chuyển chen với rời nhóm luôn còn đúng một chủ. Dòng thành viên đi theo sau; khi lệch, `owner_id` quyết định. Hội thoại tạo trước V8 dùng dòng thành viên.
- **I2 (F5):** thử lại sau khi kết nối đã gỡ mà dấu "đã đi" chưa ghi vẫn đưa người đó về offline; danh sách kết nối của node chết chỉ bị xoá sau khi mọi người được xử lý.
- **I3 (F5):** chỉ lỗi store được đọc lại; lỗi khác được đếm (`chat.presence.errors`) và bỏ qua.
- **M1, M2, M8:** xoá kênh đang mở sau khi chuyển offline; dòng thành viên và mốc vào nhóm trong một logged batch; auth-service từ chối thì vẫn trừ lượt.

Để lại (nhỏ):
- **M3:** `last_seq` đọc trước khi chèn thành viên; tin gửi trong khe đó tính là chưa đọc.
- **M4:** sau khi deploy, cặp ẩn đã xem thấy "đã nhận" = 0 cho lịch sử cũ (cột mới rỗng); bật `hideRead` cũng hạ "đã nhận" người kia thấy về mốc thật. Trong lúc rolling deploy, pod cũ vẫn còn lỗi lộ.
- **M5:** kênh đang mở tính theo người, không theo thiết bị (thiết bị B đóng kênh xoá luôn của thiết bị A).
- **M6:** cột `refuse_adds boolean` thay vì `who_can_add text`; thêm `contacts` sau này cần một migration.
- **M7:** tạo nhóm đọc quyền riêng tư tuần tự, tối đa 200 lần.
- **M9:** test listener tự đưa lại bản ghi (MockConsumer không giao lại sau seek); chưa có test nhiều partition.
- **M10:** listener ngủ tới 5 s trong luồng poll (dưới `max.poll.interval` 5 phút).
