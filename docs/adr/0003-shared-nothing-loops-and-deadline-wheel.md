# ADR 0003: State shared-nothing theo event loop + một `DeadlineWheel` cho mọi deadline

## Trạng thái

Accepted, 2026-11-05.

## Bối cảnh

Mỗi session có nhiều deadline (handshake, HELLO, heartbeat, pending ack, hết hạn, recycle, drain, slow consumer). Một timer cho mỗi deadline của mỗi session là hàng triệu task định kỳ; lock trên state dùng chung làm event loop chờ nhau.

## Quyết định

- State của session chỉ thuộc owner loop (`LoopContext`: `sessions`, `wheel`, `topics`, `pacer`, bộ đếm). Thread khác chuyển việc vào loop bằng `execute`.
- Mỗi loop có đúng một `DeadlineWheel` 128 slot, tick 1 s; session nằm trong một slot theo `nextDeadlineSec`, liên kết intrusive nên gỡ O(1) không cấp phát.
- Deadline tính lại lười tại tick; cập nhật `lastSeenSec` không đụng wheel.

## Hệ quả

- Không lock trên hot path, một task định kỳ mỗi loop.
- Độ phân giải deadline là 1 s.
- Cấu trúc dùng chung giữa các loop (PrincipalIndex, NodeState, RevocationList…) phải thread-safe và được liệt kê rõ (spec §3.4).

## Nguồn

Spec §3.3, §3.4, §3.5.
