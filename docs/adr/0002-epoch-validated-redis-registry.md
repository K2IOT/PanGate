# ADR 0002: Registry Redis hợp lệ theo epoch

## Trạng thái

Accepted, 2026-11-05.

## Bối cảnh

Push Router cần biết principal đang kết nối ở node nào. Node có thể chết mà không kịp xóa entry; entry cũ trỏ tới node đã chết hoặc incarnation cũ của chính node đó sẽ làm push đi sai chỗ.

## Quyết định

- Mỗi lần start, node lấy `epoch = INCR node:epoch:{nodeId}` và giữ lease `node:{nodeId}` (15 s, gia hạn mỗi 5 s).
- Entry `reg:{principalId}` ghi kèm `nodeId` và `epoch`. Router chỉ tin entry có epoch bằng epoch của lease đang sống.
- Mất lease quá 15 s → khi Redis hồi phục lấy epoch mới và `RegistryResync` ghi lại mọi session ACTIVE (300 session/s mỗi loop).

## Hệ quả

- Entry mồ côi tự vô hiệu khi lease hết hạn, không cần dọn đồng bộ lúc node chết.
- Sau khi mất lease có một khoảng resync; push trong khoảng đó có thể đi đường offline.

## Nguồn

Tài liệu gốc §6; spec §6.1, §6.2.
