# ADR 0006: `gw.control` đọc bằng `assign` + `seekToBeginning`, không commit

## Trạng thái

Accepted, 2026-11-05.

## Bối cảnh

Topic `gw.control` (compacted) chứa Revoke, CellMap, TenantCfg. Mỗi node cần toàn bộ state, không chia partition với node khác; consumer group thông thường sẽ chia partition và commit offset.

## Quyết định

- `KafkaConsumer` với `group.id=gw-<nodeId>` (chỉ để khớp ACL), `enable.auto.commit=false`, `assign()` mọi partition, `seekToBeginning` khi boot.
- Catch-up tới snapshot `endOffsets` lúc boot rồi mới ready; quá 5 phút thì exit.

## Hệ quả

- State luôn dựng lại từ log compacted; không có offset để hỏng.
- Boot đọc lại toàn bộ topic; thời gian boot tăng theo kích thước log compacted.

## Nguồn

Spec §6.8, §10.
