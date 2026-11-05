# ADR 0004: `connId` tự định tuyến (ULID nhúng loop và key)

## Trạng thái

Accepted, 2026-11-05.

## Bối cảnh

Push unicast tới một kết nối cụ thể cần tìm session từ `conn_id`. Một index toàn cục `connId → session` tốn bộ nhớ cho mỗi kết nối và phải đồng bộ giữa các loop.

## Quyết định

`ConnId` là chuỗi 26 ký tự Crockford base32 cấu trúc giống ULID: `ts48 | loopIdx8 | sessionKey48 | rand24`. `ConnId.loopIdx` và `ConnId.sessionKey` giải mã O(1); `sessionKey = loopIdx << 40 | counter` theo từng loop.

## Hệ quả

- Push unicast đi thẳng tới loop đúng và tra map của loop, không cần index toàn cục.
- Sắp xếp được theo thời gian, duy nhất toàn cục.
- Định dạng `connId` thành một phần contract nội bộ; đổi layout bit là thay đổi phá vỡ.

## Nguồn

Spec §3.6.
