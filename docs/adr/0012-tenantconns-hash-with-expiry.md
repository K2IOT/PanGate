# ADR 0012: `tenantconns:{t}` là hash tổng theo node, hạn 30 s

## Trạng thái

Accepted, 2026-11-05.

## Bối cảnh

Tenant cap cần tổng kết nối của tenant trên cả cell. `INCR`/`DECR` theo từng kết nối để lại số đếm thừa vĩnh viễn khi một node bị kill.

## Quyết định

- `tenantconns:{t}` là hash `node → "total|expiresAtMs"`; mỗi node ghi tổng của mình mỗi 10 s, hạn 30 s, và xóa field khi tổng về 0.
- Hết quota local thì mượn bằng Lua BORROW: cộng tổng còn hạn của node khác với tổng mới của node này; > `maxConns`, Redis lỗi hoặc quá 200 ms → close 4013.

## Hệ quả

- Node chết tự rơi khỏi tổng sau tối đa 30 s.
- Tổng có thể lệch trong cửa sổ 10–30 s; quota local `ceil(maxConns × 1,1 / nodesInCell)` hấp thụ phần lệch.

## Nguồn

Spec §4.6 bước i, §14.11; plan M3b ruling R2.
