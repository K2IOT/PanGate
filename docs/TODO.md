# TODO: Redis và ScyllaDB sau perf 3a–3c

Nguồn: review lưu trữ ngày 2026-11-25, sau khi merge perf 3c (K2IOT/PanGate#12).

## Kết luận

Ở mức tải đã đo, Redis và ScyllaDB chưa phải chỗ nghẽn. Nhưng chúng chưa được tối ưu hết:
- có một chỗ nghẽn ScyllaDB thật mà test tải chưa chạm tới (ghi biên nhận);
- có một giới hạn của Redis khi lên quy mô lớn (chat-service chỉ dùng một instance).

### Số đo hiện có

Ca 5 nhóm × 500 người, khoảng 6 000 lần giao/s, trên máy 4 vCPU chạy tất cả:
- Kafka, ScyllaDB và Redis: mỗi container khoảng 14 % một core.
- Client Redis và ScyllaDB: 6,5 % và 5,6 % CPU ứng dụng.
- Phần tốn CPU nằm ở các dịch vụ: router 27 %, chat 20 %, push-api 19 %, gateway 8,5 %.

**Giới hạn của số đo:** test tải không gửi biên nhận đã nhận/đã xem. Client test chỉ nhận tin, nên toàn bộ phần ghi biên nhận chưa được đo.

## Việc cần làm

### 1. Ghi biên nhận vào ScyllaDB song song (nghiêm trọng, chưa đo)

Hiện trạng:
- `ReceiptWorkerLoop` chạy trên **một luồng mỗi pod** (`chat-receipts`).
- Khi gom xong một cửa sổ, nó ghi mark của từng người **lần lượt**: 2 lệnh ScyllaDB chờ kết quả (`conv_marks` và `user_convs`), rồi 1 lệnh Redis cho badge. Mỗi người tốn khoảng 1–2 ms, tức chỉ khoảng 500–1 000 mark/s mỗi pod.
- Biên nhận chia partition theo hội thoại, nên **cả một kênh nằm trên một luồng**:
  - một bài trong kênh 100 000 người, nếu 30 000 người xem, sinh khoảng 60 000 lần ghi tuần tự, mất cỡ 30–60 s; biên nhận DM và nhóm cùng partition phải xếp hàng sau;
  - một nhóm 500 người ở 10 tin/s sinh khoảng 250 mark/s, vài nhóm như vậy là quá sức một pod.
- Spec 3c (§2) đã cố ý để vòng này ngoài phạm vi.

Việc:
- [ ] Thêm biên nhận đã nhận/đã xem vào `ChatLoadIT`, đo trước khi sửa.
- [ ] Ghi các mark của một lần gom song song, có giới hạn đồng thời (như `touchUserConvs` của 3c), chỉ ghi lại phần lỗi.
- [ ] Đo lại sau khi sửa; ghi số vào README của chat-service.

### 2. Redis Cluster cho chat-service (chặn quy mô 1 triệu người)

Hiện trạng:
- gateway, push-api và router đã dùng `RedisClusterClient`.
- chat-service vẫn dùng `RedisClient.create` (một instance) cho badge, cache thành viên, dedup, kết quả gửi và presence (`RedisBadges`, `RedisChatCache`, `RedisSendResults`, `RedisPresence`). Redis chạy một luồng, nên đó là trần cứng.
- Theo mô hình 50 tin/người/ngày, 1 triệu người cần cỡ 50–70 nghìn lệnh/s giờ cao điểm, phần lớn là script Lua, gần giới hạn một instance.

Việc:
- [ ] Viết spec riêng: hash tag cho khoá (`badge:{user}`, `members:{conv}`, `dedup:{conv}:…`), client cluster, script chỉ đụng một slot.
- [ ] IT failover Redis Cluster, như đã làm cho push-api.

### 3. Push API chuyển sang `EVALSHA` (việc nhỏ)

Hiện trạng: `RedisMessageStatus` (accepted, offline, delivered) và `RedisIdemStore` gửi nguyên văn script Lua mỗi lần push (`EVAL`), nên Redis phải băm lại script mỗi lần.

Việc:
- [ ] Chạy bằng `EVALSHA`, về `EVAL` khi Redis báo `NOSCRIPT`, như badge của chat (`RedisBadges.sha`). Thêm IT sống sót qua `SCRIPT FLUSH`.

### 4. Tài liệu

- [ ] Sửa dòng trạng thái của `docs/superpowers/specs/2026-11-03-gateway-service-design.md`: vẫn ghi "Draft, chờ duyệt" dù đã phát hành 0.1.0.

## Review hiệu năng toàn bộ (2026-11-26)

Một reviewer đọc lại toàn bộ đường đi của tin nhắn sau 3a–3c. F1–F4 và F6 đã được kiểm lại trong code. Chi phí một tin nhóm 500 (80 % offline, chưa tính biên nhận): khoảng 2 300 lệnh Redis, 1 700 lệnh ScyllaDB, 1 300 bản ghi Kafka.

| # | Mức | Vấn đề | Hướng sửa |
|---|---|---|---|
| F1 | Chặn | Fan-out đẩy RELIABLE cho từng thành viên, kể cả người offline (`FanOutPlanner.plan`) | Fan-out biết ai online (`pres:<user>`), người offline đi thẳng notifier; lâu dài: multicast |
| F2 | Chặn | Push API ghi hash trạng thái 7 trường mỗi push, TTL mặc định 86 400 s + 1 h, chat không đọc (~160 GB Redis ở 6k push/s) | Không ghi khi push không cần (`receipt`, idempotency); gộp INCR + trạng thái vào một `EVALSHA` |
| F3 | Cao | Router giữ khoá người nhận tới khi thiết bị ACK (`ackTimeout` 6 s); EPHEMERAL dùng chung khoá | Cho N tin chưa ACK mỗi khoá; tách khoá EPHEMERAL; test với RTT 150 ms |
| F4 | Cao | Fan-out `maxBatch 1`: mỗi tin chờ đủ câu trả lời, inbox, badge, mark rồi mới tới tin sau; 1-1 xếp sau push nhóm | Lô 16–32 tin, ghi inbox/badge/mark một lần ở seq cao nhất; hàng ưu tiên cho hội thoại nhỏ |
| F5 | Cao | chat-service một Redis: ~900 lần gọi Lua mỗi tin nhóm 500 | Redis Cluster; sửa `DEL` nhiều khoá (`RedisBadges.forget`), `presgrace` một ZSET, `PUBLISH` → `SPUBLISH` |
| F6 | Cao | Thông báo offline không gộp (collapse id = msgId); mỗi thông báo đọc lại `conversations`, quyền riêng tư, tắt tiếng | Gộp theo (người, hội thoại) 5–10 s, collapse id = hội thoại, cache vài giây; notify tra cứu bất đồng bộ |
| F7 | Cao | Biên nhận: một pod bão hoà ở ~1,5 nhóm 500; kênh 100 000 người ~200 s mỗi cửa sổ | Mục 1 ở trên; thêm: kênh chỉ ghi `user_convs.read_seq`, không push biên nhận |
| F8 | Trung bình | Mỗi tin 1-1 một LWT; đọc `send_dedup` trước khi ghi | Dedup song song trong lô; cache loại hội thoại ở Ingest; cân nhắc fencing theo epoch |
| F9 | Trung bình | `/v1/state`, `/v1/conversations` đọc cả partition inbox mỗi trang; N+1 cho DM | Gom bất đồng bộ, cache ngắn theo người, sau đó bảng chỉ mục có thứ tự |
| F10 | Trung bình | Presence một luồng mỗi pod, 8–10 lượt Redis mỗi sự kiện; `dropNode` 100 000 kết nối đứng ~1 phút | Một `EVALSHA` mỗi sự kiện, `dropNode` pipeline, chia `presgrace` |
| F11 | Trung bình | Một lane thử lại mãi làm dừng partition sau 1 000 bản ghi và chặn commit | Chuyển lane kẹt sang topic thử lại; cảnh báo theo `chat.pipeline.oldest` |
| F12 | Trung bình | `broadcast-rate` 10/s mỗi pod push-api; typing/biên nhận chung bucket với tin; khoá `synchronized` toàn cục | Bucket riêng cho push trạng thái; nâng mặc định; bỏ monitor trên đường nóng |
| F13 | Trung bình | 4 stream cố định: mỗi pod chat dùng tối đa 4 pod Push API | Số stream ≥ số backend, hoặc một stream mỗi subchannel |
| F14 | Trung bình | Channel notifier đọc quyền riêng tư từng người theo dõi | Đọc theo lô mỗi trang |
| F15 | Thấp | Cache registry router xoá trắng ở 100 000 mục; vài map không giới hạn; payload parse lại ~5 lần mỗi người nhận | Caffeine ~2M mục, xoá theo sự kiện CONNECTED |

Thứ tự đề xuất:
- [ ] 3d (chat): F1, F4, F7.
- [ ] 3e (push-api): F2, F12.
- [ ] 3f (notify): F6.
- [ ] 3g (router): F3.
- [ ] Spec riêng: F5 Redis Cluster, F10 presence.
- [ ] Sau: F8, F9, F11, F13–F15, multicast.

Câu hỏi mở cho F1: người có một thiết bị online và một thiết bị offline xử lý thế nào khi quyết định online/offline chuyển về chat.

Sơ đồ luồng dữ liệu: `docs/architecture/data-flow.html` (mở bằng trình duyệt, cuộn chuột để phóng to).

## Chấp nhận được, giữ nguyên

- Mỗi tin mới tốn một lần đọc `send_dedup` từ ScyllaDB khi cache Redis không có. Cần giữ để đúng khi Redis mất dữ liệu.
- Một LWT cho mỗi lô tối đa 100 tin của một hội thoại. Ở 1 triệu người, cả cụm khoảng 1 700 LWT/s, chịu được.
- Danh sách thành viên nhóm (khoảng 20 KB) đọc từ Redis mỗi tin. 3a đã cố ý không cache trong pod, để người bị xoá không còn nhận tin.

## Đã tối ưu, không làm lại

- Tra thành viên một dòng, giới hạn tần suất trước mọi lần tra (3a).
- Badge O(1) trong Redis (3a); script badge chạy bằng `EVALSHA` (minors 3a).
- Inbox ghi song song, tối đa 256 dòng mỗi pod (3c).
- Biên nhận gom theo cửa sổ: DM 500 ms, nhóm 2 s (CH3).
- `LastSeqCache` cho `lastSeq` của kênh (3a).

## Sau đó

- [ ] Multicast: đòn bẩy CPU lớn nhất cho nhóm và kênh (chi phí hiện tăng theo số người nhận ở mọi tầng).
- [ ] Đo khi chạy trên nhiều máy (scale-out chưa được đo; mọi số trên là một máy).

## Roadmap đã triển khai

| Giai đoạn | Nội dung | PR |
|---|---|---|
| Gateway 0.1.0 | M1 một node, M2a hạ tầng cụm, M2b push, M3a bảo vệ, M3b edge, M3c phát hành | K2IOT/PanGate#1 |
| Đường push | auth-service A1, contracts C1, push-router R1, Push API P1 | K2IOT/PanGate#2 |
| Push follow-ups | metric §6, IT failover Redis Cluster, ADR 0027 | K2IOT/PanGate#3 |
| CH1 Chat core | gửi, đánh số, fan-out, truy vấn, kênh | K2IOT/PanGate#4 |
| CH2 Tài khoản | account-service: đăng ký, đăng nhập, phiên thiết bị, hồ sơ, admin | K2IOT/PanGate#5 |
| CH3 Trạng thái hội thoại | đã nhận/đã xem, chưa đọc, đang gõ, online | K2IOT/PanGate#6 |
| CH4 Thông báo offline | notify-service, APNs/FCM, tắt tiếng, job cho kênh | K2IOT/PanGate#7 |
| CH4 follow-ups | các minor còn lại | K2IOT/PanGate#8 |
| Review đợt 1 | 8 sửa lỗi nghiệp vụ và quyền riêng tư | K2IOT/PanGate#9 |
| Perf 3a | tra cứu nóng, giới hạn tần suất, badge Redis | K2IOT/PanGate#10 |
| Perf 3b | pool client Push API, kiểm soát luồng, ưu tiên | K2IOT/PanGate#11 |
| Perf 3c và minors 3a–3c | pipeline theo khoá, nhóm `chat-state`, inbox ghi song song; stream push mở sẵn | K2IOT/PanGate#12 |

Kết quả đo qua các đợt (máy dev 4 vCPU):
- 1-1 ở 100 tin/s: p99 443 ms (CH1) → 33 ms (3c).
- Nhóm 500 ở 10 tin/s: p99 6,6 s → 1,4 s.
- Kênh 10 000 phiên: p99 khoảng 290 ms.
- Kênh 100 000 người offline: 100 000 thông báo trong 20 s.
