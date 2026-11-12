# ADR 0032: ScyllaDB cho tin và hội thoại; `messages` theo `(conv_id, bucket)`

## Trạng thái

Accepted, 2026-11-12.

## Bối cảnh

Mục tiêu là hàng chục triệu user, nên dữ liệu chủ yếu là các ghi nối tiếp (tin), cần đọc theo hội thoại và theo khoảng `convSeq`. PostgreSQL (đã dùng cho auth) phải tự chia shard ở quy mô này. Cassandra và ScyllaDB hợp với kiểu ghi nối tiếp và có LWT cho hàng rào `last_seq` (ADR 0030).

## Quyết định

- ScyllaDB 6.2, keyspace `chat` (`NetworkTopologyStrategy`, tắt tablets để dùng được LWT). Driver `com.scylladb:java-driver-core`.
- Bảng `messages`, khoá `((conv_id, bucket), conv_seq DESC)`, với `bucket = conv_seq / 10 000`. Phân vùng không lớn vô hạn, và đọc lịch sử chỉ chạm một hay hai phân vùng.
- `conversations` (giữ `last_seq` làm hàng rào), `members`, `user_convs` (hộp thư của user, ghi kèm `USING TIMESTAMP <seq>` để giá trị lớn nhất thắng; khi rời hội thoại thì đặt cờ `left` thay vì xoá), `dm_index` (`IF NOT EXISTS` theo cặp user), `send_dedup` (TTL 24 h).
- Migration là file CQL đánh số, áp dụng khi khởi động. Bảng `schema_version` ghi lại các file đã chạy; lệnh DDL có timeout 30 s.
- Redis chỉ làm cache: danh sách thành viên (TTL 60 s, xoá ngay khi thành viên đổi), dedup và kết quả gửi HTTP. Mất Redis chỉ làm chậm, không mất dữ liệu.

## Hệ quả

- Ghi tin là ghi thường; chỉ cấp số mới dùng LWT, và mỗi lô dùng một LWT.
- `user_convs` được sắp theo `last_msg_at` trong bộ nhớ. Ổn ở CH1 vì mỗi user chỉ có vài trăm hội thoại; nếu nhiều hơn thì cần bảng chỉ mục riêng.
- Thêm một hệ lưu trữ phải vận hành (devenv: `scylla` một node, `--smp 1`).

## Nguồn

Spec chat-core §0 (D6), §5.
