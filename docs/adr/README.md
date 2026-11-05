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
