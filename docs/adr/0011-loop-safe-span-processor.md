# ADR 0011: Span OTel qua `LoopSafeSpanProcessor` thay vì `BatchSpanProcessor`

## Trạng thái

Accepted, 2026-11-05.

## Bối cảnh

Span `gw.push.handoff` / `gw.push.write` kết thúc trên event loop. `BatchSpanProcessor` của OTel SDK 1.49 luôn đi qua một `ArrayBlockingQueue` có lock (kể cả khi có jctools), nên park thread `gw-io-*` khi nhiều thread cùng kết thúc span; BlockHound đã bắt được. Lần đầu tạo span trên event loop còn khởi tạo context storage (I/O).

## Quyết định

- `LoopSafeSpanProcessor`: hàng đợi lock-free có giới hạn (16 384), đầy thì bỏ span và đếm; một daemon thread `gw-otel` export theo lô mỗi 1 s qua OTLP/HTTP (sender JDK).
- `Context.current()` được gọi lúc khởi động, ngoài event loop.
- Không có endpoint (`gateway.tracing.otlp-endpoint` rỗng) thì tracing là no-op.

## Hệ quả

- Event loop không bao giờ chờ lock vì tracing.
- Span có thể bị bỏ khi export không kịp; số bị bỏ được đếm (`dropped()`), chưa xuất thành metric.
- Một class tự viết thay cho thành phần chuẩn của SDK.

## Nguồn

Spec §8; plan M3b (review minor 7).
