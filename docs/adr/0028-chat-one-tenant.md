# ADR 0028: App chat là một tenant PanGate cố định; chat-service chỉ biết user

## Trạng thái

Accepted, 2026-11-12.

## Bối cảnh

PanGate là nền tảng đa tenant: gateway, Push API và router đều làm việc theo `tenant_id` và principal `<tenant>:<id>`. App chat sắp xây là **một** sản phẩm cụ thể, không phải nền tảng chat cho nhiều khách hàng. Nếu chat-service cũng mang tenant thì mọi bảng, khoá, cache và API đều thêm một chiều chẳng ai dùng.

## Quyết định

- App chat là tenant `chat` của PanGate. Mọi người dùng là principal `chat:<userId>`.
- chat-service chỉ biết `userId`: bỏ tiền tố `chat:` khi nhận lệnh hay token, thêm lại khi gọi Push API. Không có cột tenant trong ScyllaDB và không có khoá tenant trong Redis.
- API HTTP nhận connect token (`aud=pangate-gw`) của auth-service. Token phải có `tenant=chat`, `sub=chat:<userId>`, `device` và `jti`. Thu hồi user, thiết bị hay token dùng cơ chế revoke sẵn có trên `gw.control`.

## Hệ quả

- Schema và API gọn; một user là một hàng.
- Khoá một user là revoke `chat:<userId>`; chat-service đọc lại `gw.control` khi khởi động trước khi báo ready.
- Muốn nhiều app chat riêng về sau thì mỗi app là một tenant mới, và chat-service phải thêm lại cột tenant. Giữ tiền tố `chat:` để mở đường cho việc đó.

## Nguồn

Spec chat-core §0 (D1), §1.
