# ADR 0019: Router dùng Parallel Consumer, thứ tự KEY, imperative trên virtual threads

## Trạng thái

Accepted, 2026-11-10.

## Bối cảnh

Mỗi `PushRecord` RELIABLE có thể chờ ACK tới `ack-timeout` (6 s) cộng hai lần retry. Với consumer Kafka thường, một record chậm chặn cả partition. Router cần hàng chục nghìn record đang chờ cùng lúc mà vẫn giữ thứ tự theo principal.

## Quyết định

- Dùng Confluent Parallel Consumer, ghim 0.5.3.3, `ProcessingOrder.KEY`, `CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS`, `maxConcurrency` 20 000.
- Giữ mô hình imperative (`poll`), không dùng Reactor hay Vert.x. `VirtualThreadParallelConsumer` override `setupWorkerPool(int)` để worker là virtual thread, nên một record chờ ACK chỉ chiếm một virtual thread.
- `DeliveryJob.run` chặn cho tới khi record xong. Lỗi tạm (Redis, produce outcome) được ném ra để PC retry. Khi tắt mà job bị interrupt, job không ghi outcome, nên record được xử lý lại.

## Hệ quả

- Thứ tự theo key được giữ mà không chặn các key khác.
- `setupWorkerPool` là API protected: phải kiểm tra lại mỗi lần nâng version PC (`VirtualThreadParallelConsumerTest`).
- Offset chỉ commit những record đã xong. Record dở dang được giao lại sau restart; gateway và client dedup theo `msg_id`.

## Nguồn

Spec push-router §1, §2, §4; plan R1 Tasks 2, 5, 6.
