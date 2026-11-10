# ADR 0014: Token API và connect token dùng chung khoá, tách bằng `aud`

## Trạng thái

Accepted, 2026-11-10.

## Bối cảnh

Auth cấp hai loại token:
- token API cho tenant backend (client credentials, 15 phút);
- connect token cho SDK (10 phút).

Push API (sub-project 4) phải kiểm tra token API offline, giống cách gateway kiểm tra connect token.

## Quyết định

- Cả hai đều là JWT ES256, ký bằng cùng key `active` và cùng được phát qua một JWKS.
- Token API mang `aud=pangate-api`, `sub=client:<id>`, `tenant`, `client_id`, `scope`.
- Connect token mang `aud=pangate-gw` và các claim mà `JwtVerifier` của gateway yêu cầu.
- Mỗi bên kiểm tra `aud` của mình, nên token API gửi vào HELLO bị gateway từ chối (`wrong audience`). Contract test chạy với `JwtVerifier` thật, lấy từ jar `verifier` của gateway.

## Hệ quả

- Chỉ có một bộ khoá để xoay và một JWKS để cache.
- Lộ một key thì cả hai loại token cùng bị ảnh hưởng, và `/admin/v1/keys:rotate-now` xử lý cả hai.
- Thu hồi một client cụ thể dùng khoá `revoke:c:<client_id>` (gateway bỏ qua khoá này, Push API thực thi), thay vì đổi key.

## Nguồn

Spec auth §4.1–4.3, §4.6, §10 (D5); plan A1 Tasks 1, 4, 7, 8.
