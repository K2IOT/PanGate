# ADR 0005: Hai Kafka producer (reliable / telemetry) thay vì một

## Trạng thái

Accepted, 2026-11-05.

## Bối cảnh

Upstream `cmd`, `cmd_result` và lifecycle cần `acks=all` + idempotent; telemetry cần thông lượng và chấp nhận mất. Một producer Kafka không thể vừa idempotent `acks=all` vừa `acks=1`.

## Quyết định

- Producer `reliable`: `acks=all`, `enable.idempotence=true`; producer `telemetry`: `acks=1`, không idempotent. Cả hai `buffer.memory=128 MB`, `linger.ms=5`, `compression.type=zstd`, `max.block.ms=0`.
- Buffer > 90% → `autoRead=false` cho 1% session ồn nhất mỗi loop; < 50% mở lại.

## Hệ quả

- Telemetry không làm chậm hay chặn đường `cmd`.
- Hai bộ buffer, hai bộ metric; khác với tài liệu gốc (một producer).

## Nguồn

Spec §5.5, §14.1.
