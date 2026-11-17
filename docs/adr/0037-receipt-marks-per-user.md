# ADR 0037: Mốc đã nhận / đã xem theo người, chỉ tăng bằng `USING TIMESTAMP`, không dùng receipt của router

## Trạng thái

Accepted, 2026-11-17.

## Bối cảnh

App cần "Đã nhận / Đã xem" theo người, không theo thiết bị. `DeliveredRecord` của push-router tính cho từng tin và từng thiết bị: nhóm 500 người là 500 bản ghi Kafka mỗi tin. Mốc tới trễ, trùng hoặc sai thứ tự không được làm mốc lùi.

## Quyết định

- Mỗi người trong mỗi hội thoại có hai mốc `delivered_seq`, `read_seq`; client gửi `chat.delivered` / `chat.read` (hoặc HTTP), `Ingest` kiểm tra rồi ghi `chat.receipts` (key hội thoại).
- `ReceiptWorker` gộp theo hội thoại (DM 500 ms, nhóm 2 s, kênh ngay) rồi ghi ScyllaDB: mỗi cột mốc chỉ được ghi `USING TIMESTAMP <giá trị mốc>`, nên ghi nhỏ hơn bị bỏ, không cần LWT.
- Mốc theo thành viên nằm ở bảng riêng `conv_marks` không bao giờ bị xoá: CH1 xoá dòng `members` khi rời nhóm, tombstone theo giờ thật sẽ che mãi các ô `USING TIMESTAMP <seq>` sau khi vào lại.
- Người gửi tự có mốc bằng tin của mình (fan-out ghi), nên tin của mình không tính là chưa đọc.
- Đẩy cập nhật EPHEMERAL: DM và nhóm ≤ 50 có danh sách; nhóm lớn hơn chỉ tra `readCount` theo yêu cầu; kênh không có.

## Hệ quả

- Nhóm 50 người sôi nổi tốn tối đa 50 push mỗi 2 s.
- Trạng thái đẩy bị mất (Push API lỗi) không thử lại mãi: client lấy lại qua `/v1/state` hoặc `GET …/receipts`.

## Nguồn

Spec CH3 §3, §4; ledger CH3 Task 2 (conv_marks).
