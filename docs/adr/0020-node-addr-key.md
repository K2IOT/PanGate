# ADR 0020: Gateway công bố `node:addr:{id}`

## Trạng thái

Accepted, 2026-11-10.

## Bối cảnh

Router phải mở một stream gRPC tới mỗi gateway đang sống. Registry 0.1.0 có `nodes:live` và `node:{id}` (epoch, draining) nhưng không có địa chỉ gRPC.

## Quyết định

- Sau khi server gRPC lắng nghe, gateway ghi `node:addr:{id}` = `host:port` với TTL bằng lease, gia hạn cùng nhịp lease và xoá khi dừng. `host` là `gateway.grpc.advertise-host` (mặc định `${HOSTNAME}`; dev `gateway`, local `127.0.0.1`).
- Router đọc `nodes:live` (cửa sổ 30 s), `node:{id}` và `node:addr:{id}`. Node không có địa chỉ thì không có trong danh bạ.
- Thay đổi chỉ là thêm vào: gateway cũ không ghi key này thì router bỏ qua node đó.

## Hệ quả

- Không cần service discovery riêng; danh bạ làm mới mỗi giây.
- Đổi epoch hoặc mất lease thì router đóng link, rồi phát `NODE_DOWN`.

## Nguồn

Spec push-router §3.1, §3.2; plan R1 Tasks 1, 3.
