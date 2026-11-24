# Đợt 3c: vòng Kafka thành pipeline theo khoá (ingest, sequencer, fan-out, offline)

- **Trạng thái:** Đã làm (2026-11-24, plan `docs/superpowers/plans/2026-11-24-perf-3c-kafka-pipelines.md`). Đã qua builder check (§7). Số đo ở §8.
- **Nguồn:** review toàn bộ chat 2026-11-19: CH1 I3 (độ trễ 1-1 p99 443 ms, "stop-and-wait step loop"), CH1 I1 (ghi inbox nhóm 500 tuần tự), CH3 I5 và cross (lệnh `chat.presence.watch` chạy chung hàng với `chat.send`), CH4 I2 (vòng offline một luồng). Đợt 3 gồm 3a (K2IOT/PanGate#10), 3b (K2IOT/PanGate#11), **3c** (tài liệu này).
- **Quy mô mục tiêu** (chủ dự án, 2026-11-19): 1 triệu người dùng, kênh 100 000 thành viên.
- **Quyết định của chủ dự án:** tự viết pipeline cho từng partition, không dùng Confluent Parallel Consumer (2026-11-19); song song **theo hội thoại (khoá Kafka)** bên trong một partition, không chỉ theo partition (2026-11-24).

## 1. Vì sao

Ba vòng nóng của chat-service (`UpstreamCommandListener`, `SequencerLoop`, `FanOutLoop`) cùng một khuôn:

```
poll() → mỗi partition một virtual thread, record của partition chạy lần lượt
       → chờ MỌI partition xong → seek / commitSync → poll() lần sau
```

Hệ quả, theo review (suy luận từ code, chưa profile):

1. **Rào chắn mỗi poll.** Không poll tiếp khi partition chậm nhất chưa xong và `commitSync` chưa trả về. 54 ms "chờ trong `chat.commands`" ở số đo CH1 là record đến trong lúc bước trước còn chạy, không phải độ trễ Kafka. Ở 100 tin/s các lần chờ chồng lên nhau: p50 tăng ít, p99 tăng gấp đôi (219 → 443 ms; tiêu chí < 300 ms).
2. **Mọi I/O trong partition tuần tự**, kể cả giữa các hội thoại khác nhau cùng partition. Fan-out ghi dòng `user_convs` của 500 thành viên **từng người một** sau khi push: nhóm 500 chỉ chịu khoảng 2–4 tin/s, và chặn mọi chat 1-1 nằm cùng partition.
3. **Lệnh trạng thái chung hàng với `chat.send`.** `chat.presence.watch` (tới 200 mục, khoảng 1 000 lời gọi store, 3a đã giảm phần tra trùng), `chat.read`, `chat.delivered`, `chat.typing` cùng topic upstream, cùng khoá (principal), nên cùng partition và cùng hàng với tin nhắn.
4. **`OfflineNotifierLoop` một luồng**, xử lý đồng bộ từng record rồi chờ cả mẻ: giới hạn thông báo của cả nền tảng ở vài nghìn/s.

## 2. Mục tiêu, không làm, phản mục tiêu

**Mục tiêu:**
- Poll không bao giờ chờ việc xử lý. Một hội thoại chậm hay lỗi chỉ làm chậm **chính nó** (và, khi kéo dài, partition của nó sau một giới hạn có chủ đích, §4.1).
- Thứ tự giữ đúng như hôm nay: theo hội thoại cho `chat.commands` và `chat.events`, theo người gửi (principal) cho topic upstream.
- Fan-out ghi inbox của mọi thành viên **đồng thời** (có giới hạn), không từng người.
- Lệnh trạng thái không bao giờ xếp trước một tin nhắn.
- Thấy được lag: metric lag consumer theo group, tuổi của record chờ lâu nhất.

**Thành công (đo trên máy dev 4 vCPU như CH1, `ChatLoadIT`):**
- 1-1 qua WebSocket, 100 tin/s: p99 gửi → nhận **< 300 ms**.
- Nhóm 500 người, **10 tin/s** trong 10 s: mọi tin tới đủ 500 người, lag không tăng dần (trước 3c: khoảng 2–4 tin/s; số đo trước và sau ghi lại để so).
- **5 nhóm 500 người cùng lúc, mỗi nhóm 5 tin/s:** như trên. Sức chứa tăng theo số hội thoại, không bị một hội thoại giới hạn.
- Test: một hội thoại mà store luôn lỗi **không** làm chậm hội thoại khác cùng partition; một lệnh watch chậm không làm chậm `chat.send` của cùng người.

**Không làm:**
- `ReceiptWorkerLoop` (đã gom theo cửa sổ hội thoại, commit theo `firstPending`; một chỗ duy nhất xử lý mark của một hội thoại), `ChannelNotifierLoop` (mô hình job riêng), `LifecycleListener`, `AccountsListener` (lưu lượng thấp). Giữ nguyên.
- Rút ngắn chuỗi của sequencer (bỏ đọc dedup ScyllaDB khi Redis trả lời hụt, đặt trước dải seq, ghi đồng thời dedup và tin). Đo lại sau 3c rồi mới quyết.
- Đổi số partition, tên topic, contract; exactly-once; thư viện Parallel Consumer.

**Phản mục tiêu:**
- Một tin có hai seq; một tin mất (offset được commit trước khi xử lý xong).
- Đổi thứ tự tin trong một hội thoại, hoặc thứ tự gửi của một người.
- Bộ nhớ tăng không giới hạn khi một store chậm hay một hội thoại kẹt.
- Push trùng trong vận hành bình thường. (Khi rebalance hay restart, phần đã xong sau mốc commit được chạy lại, như hôm nay; §4.1.)

## 3. Cách tiếp cận đã cân nhắc

| Cách | Được | Mất |
|---|---|---|
| **1. Tự viết pipeline cho từng partition, song song theo khoá** — **chọn** | Không thêm thư viện; giữ các bất biến mà test hiện có bảo vệ (dedup, thứ tự, commit sau khi xong); hội thoại chậm chỉ chậm chính nó | Phải tự theo dõi offset theo khoá; record đã xong sau mốc commit chạy lại khi rebalance |
| 1b. Như 1 nhưng chỉ song song theo partition | Đơn giản hơn | Nhóm 500 vẫn chặn chat 1-1 cùng partition (chủ dự án loại, 2026-11-24) |
| 2. Confluent Parallel Consumer | Song song theo khoá có sẵn | Dependency lớn, viết lại mọi vòng, cách commit khác hẳn (chủ dự án loại, 2026-11-19) |
| 3. Chỉ tách bước nặng ra topic riêng | Ít sửa | Không bỏ được rào chắn và I/O tuần tự; 443 ms vẫn còn. Chỉ dùng ý này cho lệnh trạng thái (§4.4) |

## 4. Thiết kế

### 4.1 `KeyedPipeline`: khung chung (gói `kafka`)

Một lớp Java thuần, dùng lại cho bốn vòng. Mỗi vòng đưa vào một `Consumer<String, byte[]>`, topic, tên (cho metric và log) và một **handler** xử lý một mẻ record **cùng khoá**.

**Luồng poll (platform thread, chỉ nó chạm consumer):**
- `poll(100 ms)`; mỗi record được ghi vào bộ theo dõi offset của partition (tập offset **đang chờ**) rồi xếp vào **làn** `(partition, khoá)`. Khoá null dùng một làn chung của partition. Làn đang rảnh thì khởi chạy (virtual thread).
- **Áp lực ngược:** một partition có ≥ `max-pending-per-partition` (mặc định 1 000) record đang chờ thì `pause()`; xuống dưới một nửa thì `resume()`. Đây là giới hạn bộ nhớ, và cũng là giới hạn số record đã xong phải chạy lại khi rebalance.
- **Commit:** mỗi `commit-interval` (200 ms), với mỗi partition, mốc = offset đang chờ nhỏ nhất, hoặc offset sau record cuối đã nhận khi không còn gì chờ; `commitAsync` các mốc đã đổi. Không bao giờ commit qua một record chưa xong.
- Gauge lag: tổng `consumer.currentLag(tp)` của các partition được giao (cập nhật mỗi lần poll).

**Làn (virtual thread):**
- Lấy tối đa `max-batch` record đầu hàng của làn (mặc định 100, fan-out và offline dùng 1), gọi handler, xong thì báo các offset là đã xong. Hết việc thì làn kết thúc (làn mới tạo lại khi có record).
- Thứ tự trong làn giữ nguyên; các làn chạy song song, tối đa `max-concurrency` làn đang gọi handler cùng lúc trên một pipeline (mặc định 256, semaphore) để bảo vệ store.
- **Lỗi:** handler ném lỗi thì **thử lại chính mẻ đó tại chỗ** (mẻ đóng băng: không thêm record mới vào mẻ đang thử lại) sau một khoảng chờ tăng dần 100 ms → 5 s; không tua lại partition, làn khác không bị ảnh hưởng. Trước mỗi lần thử lại, làn kiểm partition còn được giao và pipeline chưa đóng; nếu không thì bỏ mẻ (chủ mới chạy lại).
- **Record độc (tuỳ pipeline):** pipeline nào bật DLQ (sequencer, ingest, state) thì lỗi tất định lặp lại `PoisonGuard.MAX_ATTEMPTS` lần liên tiếp (theo record đầu mẻ) làm lần sau gọi handler ở chế độ **"lần cuối"**: handler xử lý từng record một và tự đưa record lỗi vào `chat.commands.dlq`, như hôm nay. Fan-out và offline **không có** DLQ, như hôm nay: lỗi nào cũng thử lại mãi (quyết định CH1 đã ghi: lỗi ở đó thực tế là tạm thời); hội thoại kẹt lộ ra qua `chat.pipeline.oldest`. Lỗi tạm thời (store, Kafka, Push API, fence) thử lại mãi ở mọi pipeline, có metric.
- Một hội thoại kẹt mãi: các làn khác tiếp tục tới khi partition đạt `max-pending-per-partition`, rồi partition dừng. Đây là giới hạn có chủ đích (hôm nay partition dừng ngay).

**Rebalance:** `onPartitionsRevoked`: làn của các partition bị thu hồi không lấy mẻ mới; chờ tối đa `revoke-wait` (5 s) cho các mẻ đang chạy; `commitSync` mốc của các partition đó; bỏ hàng đợi; gọi hook của vòng (sequencer: `Sequencer.forget()`). Mẻ còn chạy quá hạn bị `interrupt()` (cách để handler có vòng thử lại riêng, như fan-out, biết phải dừng) và kết quả không được commit (chủ mới chạy lại, dedup lo phần trùng). **Không bao giờ có hai mẻ cùng khoá chạy cùng lúc trên một pod:** nếu partition được giao lại cho chính pod này khi mẻ cũ của một khoá còn chạy, làn mới của khoá đó chờ mẻ cũ xong rồi mới bắt đầu. Assignor giữ mặc định của client Kafka.

**Đóng:** dừng poll, chờ các làn tối đa `revoke-wait`, `commitSync`, đóng consumer.

### 4.2 Sequencer (`chat.commands`, khoá = hội thoại)

- Handler nhận mẻ của **một hội thoại** (tối đa 100) → một lần `Sequencer.process` (một LWT cho cả mẻ, như hôm nay theo hội thoại) → `events.sendAll` → kết quả HTTP. Record hỏng → DLQ ngay, không tính là lỗi.
- `FencedException`: thử lại tại chỗ (Sequencer đã quên `known`, lần sau đọc lại).
- Hai làn của **cùng hội thoại** không bao giờ chạy cùng lúc trên một pod; giữa hai pod, partition chỉ có một chủ, và LWT chặn phần còn lại như hôm nay.

### 4.3 Fan-out (`chat.events`, khoá = hội thoại)

- Handler nhận **một** event mỗi lần (thứ tự push theo người nhận phụ thuộc vào thứ tự gửi vào client Push API).
- Push: như hôm nay (mọi request cùng lúc, chỉ thử lại cái lỗi; 3b đã thêm ưu tiên MUST).
- **Inbox theo lô:** thêm `ChatStores.MemberStore.touchUserConvs(List<String> users, String convId, long seq, long sentAtMs)` trả về danh sách người **chưa ghi được**. Bản ScyllaDB dùng `executeAsync`, giới hạn bằng **một semaphore chung của store** `chat.fanout.inbox-concurrency` (mặc định 256 câu đang chờ cho cả pod, không phải mỗi lần gọi), để 256 làn fan-out không thành 256 × 64 câu. Fan-out thử lại riêng những người chưa ghi, với khoảng chờ như hôm nay. Bản fake trong test giữ cùng hợp đồng.
- Mark của người gửi, badge: như hôm nay, sau dòng inbox.
- **Bỏ** `GIVE_UP` (30 s) và `STEP_BUDGET`: bước nào lỗi thì thử lại đúng bước đó, không gửi lại push đã xong; poll không bao giờ bị giữ, nên `max.poll.interval` không còn là rủi ro.

### 4.4 Ingest và lệnh trạng thái (topic upstream, khoá = principal)

- Group `chat-ingest` (pipeline, khoá principal, mẻ 1): chỉ `chat.send`, `chat.channel.open`, `chat.channel.close`. Bỏ qua lệnh trạng thái (đếm là `ignored`).
- Group **mới** `chat-state` (pipeline riêng, consumer riêng, khoá principal, mẻ 1): `chat.delivered`, `chat.read`, `chat.typing`, `chat.presence.watch`; bỏ qua phần còn lại. Cùng logic `StateCommands` như hôm nay. Cấu hình `chat.kafka.state-group` (mặc định `chat-state`).
- `chat-state` dùng `auto.offset.reset = latest`: lần triển khai đầu không phát lại lệnh trạng thái cũ trong retention (gõ phím, watch cũ vô nghĩa; mark bị lỡ thì client gửi lại ở lần đọc sau).
- Lúc chuyển đổi (pod cũ và pod mới cùng chạy), một lệnh trạng thái có thể được xử lý hai lần: vô hại (mark lấy max, gõ phím và watch lặp lại không đổi kết quả).

### 4.5 Offline notifier (`cN.push.offline`, khoá = người nhận hoặc topic)

- Pipeline, mẻ 1: `OfflineNotifications.handle` → `sink.send(...)` chờ ack Kafka. Lỗi thì thử lại tại chỗ; notify-service đã lọc trùng theo request id nên lần gửi lặp vô hại.

### 4.6 Cấu hình (`chat.pipeline.*`, dùng chung cho bốn vòng)

| Khoá | Mặc định | Ý nghĩa |
|---|---|---|
| `max-pending-per-partition` | 1000 | record đang chờ tối đa mỗi partition trước khi `pause()` |
| `max-batch` | 100 | mẻ tối đa mỗi làn (sequencer); fan-out, ingest, state, offline luôn 1 |
| `max-concurrency` | 256 | làn chạy handler cùng lúc mỗi pipeline |
| `commit-interval` | 200ms | chu kỳ commit mốc |
| `revoke-wait` | 5s | chờ mẻ đang chạy khi thu hồi partition hoặc đóng |

Thêm `chat.fanout.inbox-concurrency` (256, cho cả pod) và `chat.kafka.state-group` (`chat-state`). `max.poll.records` của các consumer giữ như hôm nay.

### 4.7 Metric

Theo tag `pipeline` (`ingest`, `state`, `sequencer`, `fanout`, `offline`):
- `chat.pipeline.pending` (gauge), `chat.pipeline.lanes` (gauge, làn đang có việc), `chat.pipeline.paused` (gauge, số partition đang pause);
- `chat.pipeline.batch` (timer, thời gian handler), `chat.pipeline.retries{reason=transient|poison}` (counter);
- `chat.pipeline.oldest` (gauge, tuổi ms của record chờ lâu nhất: thấy hội thoại kẹt);
- `chat.kafka.lag` (gauge, tổng lag các partition được giao).

Các metric cũ giữ (`chat.sequencer.lag`, `chat.fanout.lag`, `chat.send.latency`, …); bỏ `chat.fanout.budget`.

## 5. Kiểm chứng

- **`KeyedPipelineTest` (MockConsumer, đồng hồ thật, handler giả):**
  - thứ tự giữ trong một khoá; hai khoá cùng partition chạy song song (một handler chặn, khoá kia vẫn xong);
  - mốc commit không vượt record chưa xong; commit tiến khi record cũ nhất xong;
  - pause ở `max-pending-per-partition`, resume dưới một nửa; poll vẫn được gọi khi mọi làn đang bận;
  - lỗi tạm thời thử lại tại chỗ, không seek; lỗi tất định `MAX_ATTEMPTS` lần thì chế độ lần cuối;
  - thu hồi partition: chờ mẻ đang chạy, commit, bỏ hàng; hook được gọi;
  - `max-concurrency` không bị vượt.
- **Vòng:** test hiện có của sequencer, fan-out, ingest, offline, poison chuyển sang pipeline, giữ nguyên ý (dedup khi chạy lại, mark người gửi, DLQ). Test mới: fan-out ghi inbox 500 người đồng thời và chỉ thử lại người lỗi; ingest bỏ qua lệnh trạng thái, `chat-state` bỏ qua `chat.send`.
- **IT:** một hội thoại mà store lỗi (ScyllaDB fake hoặc hook lỗi theo hội thoại) không làm chậm hội thoại khác cùng partition; lệnh watch chậm không làm chậm `chat.send` của cùng người. Các IT hiện có xanh.
- **Đo:** `ChatLoadIT` thêm ca 1-1 100 tin/s, nhóm 500 ở 10 tin/s và 5 nhóm 500 mỗi nhóm 5 tin/s; ghi số trước và sau vào README chat-service.
- `make smoke` xanh.

## 6. Để người xây dựng tự quyết

Cấu trúc dữ liệu cho tập offset đang chờ; dọn làn rảnh; tên lớp phụ; cách chia handler từng vòng thành lớp; cách gắn hook lỗi theo hội thoại cho IT.

## 7. Sau builder check (2026-11-24)

Builder check (một người xây dựng đọc tài liệu mà không hỏi được ai) hỏi ba điều; cả ba là quyết định của người viết, không đổi điều chủ dự án muốn:

- **Mục tiêu nhóm 500 ở 20 tin/s không đạt được nếu fan-out giữ thứ tự từng event trong một hội thoại** (mỗi event phải xong trong 50 ms, trong khi CH1 đo "event → Push API trả lời" khoảng 77 ms riêng cho 1-1). Giữ thứ tự chặt (phản mục tiêu §2), hạ mục tiêu một nhóm xuống **10 tin/s** và thêm ca **5 nhóm cùng lúc**: điều cần chứng minh là hội thoại chậm không chặn hội thoại khác và sức chứa tăng theo số hội thoại. Một nhóm 500 người hiếm khi vượt vài tin/s. Nếu đo cho thấy 10 tin/s cũng không đạt, ghi số và hỏi chủ dự án trước khi cho các event của một hội thoại chồng nhau.
- **Record độc ở fan-out và offline:** hôm nay hai vòng này không có DLQ và thử lại mãi. Giữ như vậy (§4.1), không thêm topic DLQ mới; `chat.pipeline.oldest` cho thấy hội thoại kẹt.
- **Mẻ thử lại** đóng băng cho tới khi xong hoặc bị đưa vào DLQ (§4.1).

Thêm vào thiết kế từ các điểm nhỏ của builder check: hai mẻ cùng khoá không bao giờ chạy cùng lúc trên một pod khi partition được giao lại (§4.1); làn ngừng thử lại khi mất partition hay pipeline đóng (§4.1); giới hạn ghi inbox là chung cho cả pod (§4.3); assignor giữ mặc định.

## 8. Số đo (2026-11-24, máy 4 vCPU, `ChatLoadIT`)

| Ca | Trước 3c | Sau 3c |
|---|---|---|
| 1-1, 100 tin/s | p99 194,7 ms | p99 33,3 ms |
| Nhóm 500, 10 tin/s | đủ, p99 6,6 s, tin cuối tới 6,6 s sau lần gửi cuối (fan-out tụt dần) | đủ, p99 1,4 s, tin cuối tới 0,3 s sau lần gửi cuối (theo kịp) |
| 5 nhóm 500, mỗi nhóm 5 tin/s | không có số hợp lệ (client test không heartbeat bị đóng) | đủ 125 000, p99 8,7 s, tin cuối tới 10 s sau lần gửi cuối |

- §2 "1-1 100 tin/s p99 < 300 ms": đạt (máy này nhanh hơn máy đo CH1, nên số gốc cũng đã đạt).
- §2 "nhóm 500 ở 10 tin/s, lag không tăng dần": đạt.
- §2 "5 nhóm × 5 tin/s": mọi tin tới đủ, nhưng hàng đợi dồn trong 10 s rồi rút: 12 500 push/s vượt sức của cả stack chạy chung trên 4 lõi (khoảng 6 000/s). Giới hạn là CPU của máy chung, không phải thứ tự theo hội thoại, nên không nới thứ tự (§7).
