# ADR 0027: Thứ tự ORDERED chỉ được đảm bảo trong một pod Push API

## Trạng thái

Accepted, 2026-11-11.

## Bối cảnh

Push API cấp seq (`INCR seq:{p}`) trước khi produce. Các push cùng một principal tới **cùng một pod** đi qua một hàng đợi theo target (HTTP đơn, batch và `SendStream`), nên thứ tự trong Kafka khớp thứ tự seq. Hai pod nhận push cho cùng một principal cùng lúc thì không có hàng đợi chung: request nhận seq 5 có thể vào Kafka sau request nhận seq 6. Router giữ thứ tự Kafka theo key, gateway không sắp xếp lại. Spec chưa nói ORDERED có nghĩa là "giao đúng thứ tự seq" giữa nhiều pod hay không.

Cân nhắc ba hướng:
1. Ghi rõ đây là giới hạn; client sắp lại theo seq.
2. Load balancer định tuyến mọi push của một principal tới cùng một pod (consistent hashing). Không đổi code, nhưng ràng buộc cách triển khai.
3. Cấp seq ở router (đã tuần tự theo key). Đổi contract: response của Push API không còn trả seq ngay.

## Quyết định

Hướng 1.
- Thứ tự giao của ORDERED khớp thứ tự seq cho các push mà **một pod** nhận; giữa nhiều pod thì không đảm bảo.
- Client (SDK) dựa vào `seq`/`seq_epoch` để phát hiện lỗ hổng và sắp lại; Inbox & Sync (Phase 2) là nơi giải quyết triệt để, vì client lấy lại tin theo seq từ log.
- Tenant cần thứ tự chặt cho một principal nên gửi các push đó tuần tự (chờ `202` trước khi gửi push sau), hoặc trong một batch.

## Hệ quả

- Không thêm ràng buộc hạ tầng, không đổi contract.
- Một tenant gửi song song qua nhiều pod có thể thấy client nhận seq 6 trước seq 5.
- Xem lại khi làm Inbox & Sync; hướng 2 hoặc 3 vẫn mở nếu số đo cho thấy cần.

## Nguồn

Spec push-api §2.4; review luồng P1 và quyết định của chủ dự án 2026-11-11.
