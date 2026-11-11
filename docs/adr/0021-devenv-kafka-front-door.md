# ADR 0021: Devenv dùng Kafka làm cửa vào cho tới khi có Push API

## Trạng thái

Accepted, 2026-11-10.

## Bối cảnh

Push API (sub-project 4) chưa có, nhưng router cần được chạy thử đầu-cuối trong local, dev và smoke.

## Quyết định

- `devtools push` dựng `PushRecord` như Push API sẽ làm: `msg_id` ULID, `INCR seq:{p}` cho RELIABLE/ORDERED tới principal (`seq_epoch` 0), kiểm tra bằng `PushRecordRules`, key và header theo testkit. Sau đó produce vào `c1.push.requests`.
- `fake-pushapi --kafka` biến KICK của gateway thành `PushRecord{CONTROL, kick}` trên Kafka và trả `CODE_OK` khi Kafka ack. Chế độ `--router` (gọi thẳng gateway) vẫn giữ cho môi trường không có router.
- `make smoke` chạy `--kafka`: PUSH qua router rồi ACK, và OFFLINE (`REASON_NO_SESSION`) cho principal không có phiên. `--no-router` giữ bước fake-router cũ.
- devtools phụ thuộc `platform-contracts-testkit` ở scope compile (bỏ testcontainers và junit).

## Hệ quả

- Đường push được kiểm chứng trong devenv trước khi có Push API.
- Khi Push API ra đời, `devtools push` và `fake-pushapi --kafka` thành công cụ phát triển; quy tắc dựng record nằm ở testkit nên không bị lệch.

## Nguồn

Spec push-router §6; plan R1 Task 7.
