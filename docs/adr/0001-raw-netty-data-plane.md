# ADR 0001: Raw Netty data plane; Spring Boot chỉ lo lifecycle và config

## Trạng thái

Accepted, 2026-11-05.

## Bối cảnh

Gateway giữ khoảng 278k kết nối dài hạn mỗi node, trần 500k (tài liệu gốc §2). Mỗi kết nối phải tốn ít bộ nhớ (< 5 KB heap) và event loop không được block. Spring WebFlux/Reactor Netty và Tomcat thêm tầng trừu tượng và cấp phát trên mỗi frame; Spring Boot 3.5 còn pin Reactor Netty 1.2 cho Netty 4.1, xung đột với Netty 4.2 dùng ở data plane.

## Quyết định

- Data plane (WSS :8443, TCP :8883, SSE và `/v1/upstream`) viết thẳng trên Netty 4.2: `MultiThreadIoEventLoopGroup` + epoll, pipeline tự dựng (`AcceptGate`, `UpgradeGuard`, `FrameCodec`, `HelloAuthHandler`, `SessionHandler`).
- Spring Boot chỉ quản lý lifecycle (`SmartLifecycle`), cấu hình, metrics và cổng quản trị :8081 (Spring MVC + Tomcat, tối đa 8 thread), không nằm trên data path.
- BlockHound bật cho thread `gw-io-*` trong mọi test.

## Hệ quả

- Kiểm soát trực tiếp cấp phát, backpressure, thứ tự đóng kết nối; heap/session đo được 4,7–4,9 KB.
- Phải tự viết những gì framework web có sẵn (routing HTTP tối thiểu, CORS cho SSE/upstream, mapping lỗi).
- Không dùng được `spring-boot-starter-webflux` (enforcer cấm).

## Nguồn

Tài liệu gốc §4, §10; spec §1, §3.2, §3.7, §14.9.
