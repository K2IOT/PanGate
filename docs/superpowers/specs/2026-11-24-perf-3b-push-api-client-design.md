# Đợt 3b: client Push API của chat-service (pool, kiểm soát luồng, ưu tiên)

- **Trạng thái:** Bản thảo (2026-11-24), chờ duyệt.
- **Nguồn:** review toàn bộ chat 2026-11-19, CH1 I2 ("One Push API stream per pod"). Đợt 3 gồm 3a (đã merge, K2IOT/PanGate#10), **3b** (tài liệu này), 3c (vòng Kafka theo partition).
- **Quy mô mục tiêu** (chủ dự án, 2026-11-19): 1 triệu người dùng, kênh 100 000 thành viên.

## 1. Vì sao

`chat-service/.../grpc/PushApiClient.java` hiện mở **một** stream `PushService.SendStream` mỗi pod, kênh gRPC dùng `pick_first`, mọi lần gửi đi qua cùng một khoá. Review chỉ ra bốn vấn đề:

1. Mỗi pod chat dồn vào **một pod Push API**: thêm pod Push API không tăng sức chứa.
2. Server cho tối đa 1 000 request đang xử lý mỗi stream (`max-in-flight-per-stream`, flow control thủ công), nhưng client **không xem cửa sổ đó**: phần dư nằm trong bộ đệm gRPC phía client, không giới hạn.
3. Timeout 2 s **bắt đầu trước khi server nhận** request: request còn đợi trong hàng hết giờ, fan-out gửi lại trong khi bản gốc vẫn được xử lý → bão gửi lại.
4. Chat **không gắn `idempotency_key`** cho push, nên mỗi lần gửi lại là một push mới (seq mới, `msgId` mới trong Push API). App lọc trùng theo `ChatMessage.msg_id`, nhưng mọi tầng tốn gấp đôi.

## 2. Mục tiêu, không làm, phản mục tiêu

**Mục tiêu:**
- Sức chứa push của một pod chat tăng theo số pod Push API.
- Không bao giờ có nhiều hơn cửa sổ của server đang chờ trên một stream; phần dư nằm trong hàng đợi có giới hạn phía chat, theo mức ưu tiên.
- Gửi lại một push bắt buộc không sinh bản trùng ở Push API.
- Khi Push API quá tải, push ít quan trọng bị bỏ trước; tin nhắn không bao giờ bị chat bỏ.

**Không làm:** client KICK của gateway (cùng kiểu stream đơn, lượng nhỏ; để sau); đổi sang API HTTP batch; đổi contract (`PushRequest`, `PushResponse`); thay đổi phía Push API; mTLS (vẫn là TODO trước production, README).

**Phản mục tiêu:** đổi thứ tự push của **cùng một người nhận** (hoặc cùng topic); tin nhắn bị bỏ vì có trận gõ phím / online dồn dập; bộ nhớ chat tăng không giới hạn khi Push API chậm.

## 3. Cách tiếp cận đã cân nhắc

| Cách | Được | Mất |
|---|---|---|
| **A. Pool N stream, kiểm soát luồng phía client, ba mức ưu tiên** — **chọn** (chủ dự án, 2026-11-24) | Sửa cả bốn vấn đề; thêm pod Push API là thêm sức chứa; giữ thứ tự theo người nhận bằng hash | Gần như viết lại `PushApiClient`; nơi gọi phải nói mức ưu tiên |
| B. API HTTP batch `POST /v1/push:batch` | Ít request với nhóm lớn; cân bằng tải HTTP sẵn | Token API cho chat; gõ phím / online không hợp lô; độ trễ gom lô; đổi nhiều nơi gọi |
| C. Sửa tối thiểu một stream | Nhỏ | Mỗi pod chat vẫn dồn vào một pod Push API; không đạt mục tiêu quy mô |

## 4. Thiết kế

### 4.1 Ba mức ưu tiên (chủ dự án, 2026-11-24)

| Mức | Push | Khi không còn chỗ |
|---|---|---|
| **MUST** (bắt buộc tới) | tin nhắn DM / nhóm (`FanOutLoop`), bài kênh (`BROADCAST`), lệnh mở / đóng / thu hồi đăng ký kênh (`ChannelSubscriptions`, `CONTROL`) | Không bao giờ bị chat bỏ. Xếp hàng (có giới hạn); hàng đầy thì từ chối ngay (`PushUnavailableException`) để nơi gọi dừng và thử lại với backoff như hiện nay — vòng Kafka dừng đọc thêm thay vì đệm. Gửi lại với cùng `idempotency_key`. Thu hồi đăng ký kênh ở mức này vì là quyền riêng tư (người vừa rời không nhận thêm bài). |
| **BEST_EFFORT** (cố gắng tới) | biên nhận (`ReceiptWorkerLoop`: `own_read_seq`, mốc của người khác), báo gửi bị từ chối (`PushNotifier`) | Chờ trong hàng tối đa `low-wait` (2 s), rồi bỏ (`PushDroppedException`). App lấy lại trạng thái qua `/v1/state`. |
| **DROPPABLE** (bỏ ngay được) | gõ phím (`TypingRelay`), trạng thái online (`PresenceFanOut`, ảnh chụp của `PresenceWatch`) | Không còn chỗ ngay lúc gửi thì bỏ ngay, không xếp hàng. |

- **Phần dành riêng:** BEST_EFFORT và DROPPABLE **cộng lại** chỉ dùng tối đa `low-share` (20%) cửa sổ của một stream; MUST dùng được toàn bộ cửa sổ. Một trận gõ phím không thể lấy chỗ của tin nhắn.
- **Thứ tự lấy ra khỏi hàng:** khi có chỗ, MUST trước, rồi BEST_EFFORT (trong giới hạn phần dành riêng).
- Mức ưu tiên do nơi gọi nói rõ: `Pusher.send(PushRequest, Priority)`; `send(PushRequest)` cũ = MUST (để test và nơi gọi chưa đổi vẫn an toàn).

### 4.2 Pool stream và cân bằng tải

- Một `ManagedChannel` tới `chat.push-api.target` (`dns:///push-api:9443`) với chính sách **`round_robin`**: gRPC mở một kết nối (subchannel) tới mỗi địa chỉ DNS trả về và trải các call qua chúng.
- Pool có `streams` (mặc định 4) stream `SendStream` dùng lâu dài, mở lười; mỗi stream là một call nên được `round_robin` đặt lên một pod Push API (với ≥ 4 pod, mỗi pod chat dùng 4 pod khác nhau; với 1 pod, cả 4 về một pod — vẫn đúng).
- **Chọn stream theo hash** của khoá đích, giống `PushGrpcService.targetKey`: `principal_id` cho MESSAGE / CONTROL, `"topic:" + topic` cho BROADCAST. Cùng người nhận luôn đi cùng stream, nên thứ tự (server giữ thứ tự theo đích trong một stream) được giữ.
- **Ràng buộc triển khai** (ghi vào README): ở Kubernetes, `push-api` phải là **headless service** (DNS trả mỗi pod một bản ghi A); với ClusterIP một IP thì `round_robin` chỉ thấy một địa chỉ (đúng nhưng không trải tải). Kênh gRPC tự phân giải lại DNS khi một kết nối hỏng.
- Một stream kết thúc (lỗi, server `drain` lúc dừng): mọi request đang chờ trên nó lỗi ngay (`PushUnavailableException`); request còn trong hàng của stream đó **không** lỗi, mà được gửi khi stream mở lại (lần gửi tiếp hoặc ngay sau khi kết thúc). Mức MUST được nơi gọi thử lại, nhờ `idempotency_key` không trùng.

### 4.3 Kiểm soát luồng phía client

- Mỗi stream giữ số request đang chờ trả lời (`inFlight`) ≤ `window` (mặc định 1 000; **phải ≤** `max-in-flight-per-stream` của Push API). Một request chỉ được ghi lên stream khi còn chỗ; hết chỗ thì vào hàng đợi của stream đó theo mức (4.1).
- Ghi thêm cũng tôn trọng `ClientCallStreamObserver.isReady()` / `setOnReadyHandler` (bộ đệm truyền của gRPC), để không đệm ở tầng dưới.
- Hàng đợi MUST mỗi stream có giới hạn `queue-max` (mặc định 10 000); BEST_EFFORT có cùng giới hạn và hết hạn sau `low-wait`; DROPPABLE không có hàng.
- **Timeout** `chat.push-api.timeout` (2 s) **bắt đầu khi request được ghi lên stream**, không phải lúc gọi `send`. Thời gian chờ trong hàng được đo riêng (metric) và không bao giờ làm request MUST hết hạn.
- Trả lời đến (hoặc lỗi / timeout) giải phóng một chỗ và lấy request kế tiếp ra khỏi hàng.

### 4.4 `idempotency_key` cho push bắt buộc

| Push | `idempotency_key` |
|---|---|
| tin nhắn tới một thành viên | `<msgId>/<userId>` |
| bài kênh (BROADCAST) | `<msgId>` |
| tin nhắn tới một thiết bị (nếu có) | `<msgId>/<userId>/<deviceId>` |
| CONTROL mở / đóng / thu hồi kênh | `request_id` của lệnh (giữ nguyên khi gửi lại) |
| BEST_EFFORT, DROPPABLE | không đặt (Push API không dedup; bỏ được thì không gửi lại) |

Push API dedup 1 giờ theo tenant (spec Push API §2): một lần gửi lại trong vòng 1 giờ trả về cùng `msgId` / seq, không tạo record mới. Chi phí: một thao tác Redis thêm mỗi push bắt buộc ở Push API (chấp nhận).

### 4.5 Cấu hình (`chat.push-api.*`)

| Khoá | Mặc định | |
|---|---|---|
| `target`, `plaintext`, `timeout` | như hiện nay (`timeout` nay tính từ lúc ghi) | |
| `streams` | 4 | stream mỗi pod |
| `window` | 1 000 | ≤ `max-in-flight-per-stream` của Push API |
| `low-share` | 0.2 | phần cửa sổ tối đa cho BEST_EFFORT + DROPPABLE |
| `low-wait` | 2s | BEST_EFFORT chờ tối đa |
| `queue-max` | 10 000 | mỗi stream, mỗi mức có hàng |

### 4.6 Metric

`chat.push.inflight{stream}`, `chat.push.queued{priority}` (gauge), `chat.push.queue.wait{priority}` (timer, từ `send` tới lúc ghi), `chat.push.dropped{priority,reason}` (`window_full`, `waited`, `queue_full`), `chat.push.latency` (từ lúc ghi tới trả lời), `chat.push.stream.restarts`.

## 5. Kiểm chứng

- **Cửa sổ:** server giả (in-process) không trả lời → client ghi đúng `window` request rồi dừng; phần còn lại nằm trong hàng, không hết hạn sau `timeout`; server trả lời một → client ghi đúng một request kế tiếp.
- **Ưu tiên:** cửa sổ đầy MUST → DROPPABLE bị bỏ ngay (`window_full`), BEST_EFFORT bị bỏ sau `low-wait` (`waited`); khi có chỗ, MUST ra trước BEST_EFFORT; BEST_EFFORT + DROPPABLE không bao giờ vượt `low-share` cửa sổ, kể cả khi không có MUST nào.
- **Thứ tự:** nhiều push cho cùng người nhận gửi đồng thời từ nhiều thread → server thấy đúng thứ tự gọi `send` của từng thread trên một stream; người nhận khác nhau trải qua nhiều stream.
- **Stream hỏng:** server huỷ stream → request đang chờ lỗi ngay; request trong hàng được gửi trên stream mới.
- **Trải tải:** hai server in-process sau một resolver tĩnh hai địa chỉ → cả hai nhận stream.
- **Idempotency:** `FanOutPlanner` đặt đúng khoá theo bảng 4.4; IT với Push API thật (đã có trong JVM của `PushApiClientIT`): gửi lại cùng push → cùng `msgId`, một record Kafka.
- ITs hiện có (`FanOutIT`, `ReceiptWorkerIT`, `ChatE2EIT`, smoke) vẫn xanh.

## 6. Để người xây dựng tự quyết

Cấu trúc lớp (một lớp `PushStream` mỗi stream hay gộp), cấu trúc hàng đợi, cách đặt `round_robin` (`defaultLoadBalancingPolicy`), hàm hash, cách test trải tải; tên exception cho push bị bỏ (miễn phân biệt được với `PushUnavailableException` để `FanOutLoop` không coi "bỏ" là lỗi cần thử lại — MUST không bao giờ bị bỏ nên điều này chỉ ảnh hưởng BEST_EFFORT / DROPPABLE).
