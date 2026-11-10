# ADR 0018: Value registry v2 có device ở cuối

## Trạng thái

Accepted, 2026-11-10.

## Bối cảnh

Push tới một device cụ thể đòi hỏi router tìm được đúng kết nối. Tuy vậy `reg:{p}` của gateway 0.1.0 chỉ lưu `nodeId|epoch|connectedAtMs|platform`.

## Quyết định

- Gateway ghi value v2: `nodeId|epoch|connectedAtMs|platform|device`.
- `device` đứng cuối. Phía đọc tách với giới hạn 5 (`split("\\|", 5)`), nên device có chứa `|` vẫn đọc được nguyên vẹn.
- Value v1 (4 phần) vẫn được chấp nhận, nhưng không bao giờ khớp target device. Router chuyển push đó sang `push.offline` với `REASON_NO_SESSION`.
- `RegistryValues` trong testkit là parser tham chiếu và nguồn golden case cho cả gateway lẫn router.

## Hệ quả

- Router biết chính xác device có online hay không, nên đường offline vẫn đơn giản.
- Gateway có thể lên trước router.
- Entry v1 biến mất sau một lượt rolling deploy gateway, vì mọi phiên đều bị drain rồi kết nối lại.

## Nguồn

Spec push contracts §4, §5 (C2); plan C1 Tasks 4, 6.
