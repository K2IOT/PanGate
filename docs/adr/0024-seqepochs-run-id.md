# ADR 0024: Phát hiện failover Redis state qua `run_id`

## Trạng thái

Accepted, 2026-11-10.

## Bối cảnh

Sau failover, replica có thể thiếu các `INCR seq:{p}` cuối, nên seq bị cấp lại. Gateway đọc `seqepochs` (slot → epoch) để client biết dãy seq đã đặt lại. Push API là bên ghi.

## Quyết định

- Watcher (mỗi 5 s và ngay khi Lettuce kết nối lại) đọc master và dải slot của từng shard, cùng `run_id` (`INFO server`).
- Một script Lua trên `runids{seqepochs}` và `seqepochs` (cùng slot nhờ hash tag) so `run_id` đã lưu: lần đầu thấy thì ghi, khác thì ghi mới và bump mọi slot của dải. Nhiều pod thấy cùng failover chỉ bump một lần.
- Pod nhớ `run_id` và epoch đã thấy trong bộ nhớ: nếu Redis mất cả hai hash (khởi động lại không persistence), pod vẫn bump, và epoch mới lớn hơn mọi epoch pod từng thấy.
- Seq lùi với một principal pod đã phục vụ thì chạy watcher ngay và cấp lại seq.
- `POST /admin/v1/seqepochs:bump` để bump thủ công.

## Hệ quả

- Cửa sổ trùng seq sau failover thường dưới 1 s, tối đa khoảng 5 s.
- Chế độ cluster chưa có IT riêng.

## Nguồn

Spec push-api §3; plan P1 Task 8.
