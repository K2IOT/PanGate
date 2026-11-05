# ADR 0009: local = infra thật trên `localhost` + gateway từ IDE; dev = toàn bộ trong compose; Redis standalone ở cả hai

## Trạng thái

Accepted, 2026-11-05.

## Bối cảnh

Yêu cầu: code phải start được ở local và dev. Redis Cluster trong Docker announce IP container, JVM chạy trên máy không kết nối được.

## Quyết định

- local: `make infra-up` bật Kafka + Redis trên `localhost`, gateway chạy từ IDE với profile `local`.
- dev: `make images up smoke`, mọi thứ trong docker compose.
- Redis standalone ở local và dev, cluster ở staging/prod (`gateway.redis.topology`); mọi Lua script chỉ đụng một key nên chạy giống nhau ở hai mode.
- `DevBootstrap` sinh secret dev; CI job `local-start` và `dev-start` là required check.

## Hệ quả

- Một lệnh cho mỗi môi trường, kiểm chứng trong CI.
- Hành vi riêng của Redis Cluster (MOVED, slot) không được kiểm ở local/dev.

## Nguồn

Spec §9.1, §14.7.
