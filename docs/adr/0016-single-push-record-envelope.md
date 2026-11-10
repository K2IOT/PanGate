# ADR 0016: Một envelope `PushRecord` trên `cN.push.requests`

## Trạng thái

Accepted, 2026-11-10.

## Bối cảnh

Push API, Push Router và Inbox (Phase 2) cần chung một định dạng cho mọi push đi xuống: unicast tới principal, tới một device, broadcast theo topic, và control (SUBSCRIBE, KICK…). Thứ tự giữa CONTROL và MESSAGE của cùng một principal phải được giữ, ví dụ KICK hoặc REFRESH_CONFIG phải tới trước các MESSAGE gửi sau nó cho principal đó.

## Quyết định

- **Một topic, một message.** Chỉ có một topic `cN.push.requests` (64 partition) và một message `PushRecord`. Message này dùng `oneof target { principal_id | device | topic }` và `kind` ∈ MESSAGE / BROADCAST / CONTROL.
- **Key:**
  - MESSAGE và CONTROL dùng `principal_id`;
  - target device dùng principal của device;
  - BROADCAST dùng `topic:<topic>`.
- **Router ghi ra hai topic** bằng hai message riêng: `OfflineRecord` lên `cN.push.offline` và `DeliveredRecord` lên `cN.push.delivered`. Router cũng thêm `EVENT_TYPE_NODE_DOWN` vào `cN.gw.lifecycle`.

## Hệ quả

- Cùng key nên cùng partition, vì vậy thứ tự giữa MESSAGE và CONTROL của một principal được giữ. BROADCAST (key `topic:<t>`) **không** có thứ tự so với CONTROL như SUBSCRIBE: một SUBSCRIBE và broadcast đầu tiên của topic đó có thể tới gateway theo thứ tự bất kỳ.
- Mỗi luồng chỉ cần một consumer group: một cho router, sau này thêm một cho inbox.
- Broadcast dùng chung partition với unicast, nhưng key `topic:<t>` rải chúng ra nhiều partition.
- Tách topic theo kind đã bị loại, vì sẽ mất thứ tự giữa CONTROL và MESSAGE.

## Nguồn

Spec push contracts §1, §2 (C7); tài liệu gốc §3.4, §6; plan C1 Tasks 1–2.
