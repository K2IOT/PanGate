# ADR 0022: Push API là một service, một lõi `PushAcceptor` cho HTTP và gRPC

## Trạng thái

Accepted, 2026-11-10.

## Bối cảnh

Push API có hai bề mặt: `POST /v1/push` cho tenant backend và `PushService.SendStream` cho service nội bộ (KICK của gateway). Cả hai phải áp cùng luật V1–V14, cùng policy và cùng cách cấp seq.

## Quyết định

- Một module `push-api-service`: Spring MVC trên virtual threads, cổng HTTP 8080, gRPC 9443, quản trị 8081.
- Một lõi `PushAcceptor` thuần Java nhận `(Caller, PushRequest)` và trả `PushResponse`. HTTP ánh xạ JSON theo contracts §3.2, gRPC chuyển thẳng.
- `Caller` là `ApiCaller` (token API: tenant, client, scope) hoặc `InternalCaller` (SPIFFE id). CONTROL chỉ nhận qua gRPC. Kill switch và rate limit không áp cho CONTROL nội bộ.
- Consumer `gw.control`, consumer trạng thái và watcher `seqepochs` chạy trong cùng process.

## Hệ quả

- Luật và policy nằm ở một chỗ; test lõi không cần Spring.
- Tải đọc trạng thái dùng chung pod với đường accept; có thể tách sau mà không đổi contract.

## Nguồn

Spec push-api §1, §2; plan P1 Tasks 2–6, 9, 10.
