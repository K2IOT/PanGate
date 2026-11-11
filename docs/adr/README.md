# Architecture Decision Records

Quyết định kiến trúc của gateway-service (spec §13). Mỗi ADR gồm: Trạng thái, Bối cảnh, Quyết định, Hệ quả, Nguồn.

| ADR | Quyết định |
|---|---|
| [0001](0001-raw-netty-data-plane.md) | Raw Netty data plane; Spring Boot chỉ lo lifecycle và config |
| [0002](0002-epoch-validated-redis-registry.md) | Registry Redis hợp lệ theo epoch |
| [0003](0003-shared-nothing-loops-and-deadline-wheel.md) | State shared-nothing theo event loop + một `DeadlineWheel` cho mọi deadline |
| [0004](0004-self-routing-conn-id.md) | `connId` tự định tuyến (ULID nhúng loop và key) |
| [0005](0005-two-kafka-producers.md) | Hai Kafka producer (reliable / telemetry) thay vì một |
| [0006](0006-gw-control-assign-no-commit.md) | `gw.control` đọc bằng `assign` + `seekToBeginning`, không commit |
| [0007](0007-router-message-per-connection.md) | Router gửi `MESSAGE` theo từng kết nối (`conn_id` bắt buộc) |
| [0008](0008-temporary-monorepo-polyrepo-ready.md) | Monorepo tạm thời, giữ build độc lập cho từng thư mục (polyrepo-ready) |
| [0009](0009-local-and-dev-environments.md) | local = infra thật trên `localhost` + gateway từ IDE; dev = toàn bộ trong compose; Redis standalone ở cả hai |
| [0010](0010-higher-epoch-wins-lease.md) | Lease "epoch lớn hơn thắng" để restart nhanh không bị fence |
| [0011](0011-loop-safe-span-processor.md) | Span OTel qua `LoopSafeSpanProcessor` thay vì `BatchSpanProcessor` |
| [0012](0012-tenantconns-hash-with-expiry.md) | `tenantconns:{t}` là hash tổng theo node, hạn 30 s |
| [0013](0013-auth-managed-signing-keys.md) | Auth tự quản lý khoá ES256 trong PostgreSQL (KEK qua `SecretSource`, publish trước 1 h, chồng lấn 7 ngày, advisory lock) |
| [0014](0014-shared-keys-split-by-audience.md) | Token API và connect token dùng chung khoá, tách bằng `aud` (`pangate-api` / `pangate-gw`) |
| [0015](0015-revocation-outbox.md) | Thu hồi qua transactional outbox lên `gw.control`; tombstone khi hết hạn |
| [0016](0016-single-push-record-envelope.md) | Một envelope `PushRecord` trên `cN.push.requests`; key là principal hoặc `topic:<t>` |
| [0017](0017-push-api-builds-the-frame.md) | Push API dựng sẵn `Frame`; router không đọc payload |
| [0018](0018-registry-value-v2.md) | Value registry v2 có device ở cuối; router phân giải target device |
| [0019](0019-router-parallel-consumer-virtual-threads.md) | Router dùng Confluent Parallel Consumer (`KEY`), imperative trên virtual threads |
| [0020](0020-node-addr-key.md) | Gateway công bố `node:addr:{id}` để router tìm địa chỉ gRPC |
| [0021](0021-devenv-kafka-front-door.md) | Devenv dùng Kafka (`devtools push`, `fake-pushapi --kafka`) làm cửa vào cho tới khi có Push API |
| [0022](0022-push-api-one-acceptor.md) | Push API là một service; một lõi `PushAcceptor` cho HTTP và gRPC |
| [0023](0023-idempotency-reserved-values.md) | Idempotency giữ sẵn `msgId`/seq cho mỗi key (`pending` + `:last`) |
| [0024](0024-seqepochs-run-id.md) | Phát hiện failover Redis state qua `run_id`; bump `seqepochs` bằng script CAS |
| [0025](0025-message-status-in-redis.md) | Trạng thái message trong Redis, TTL theo message; `delivered` cần `receipt` |
| [0026](0026-push-rate-per-pod.md) | Rate limit push là token bucket mỗi pod, cấu hình qua `TenantLimits` |
| [0027](0027-ordered-per-pod.md) | Thứ tự ORDERED chỉ được đảm bảo trong một pod Push API; client sắp lại theo seq |
