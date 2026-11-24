# Đợt 3b: client Push API của chat-service (pool, kiểm soát luồng, ưu tiên)

- **Trạng thái:** Đã làm (2026-11-24, plan `docs/superpowers/plans/2026-11-24-perf-3b-push-api-client.md`). Khác spec: không dùng `isReady()` / `onReady` của gRPC (§4.3), vì cửa sổ đã giới hạn bộ đệm và chờ `isReady()` làm stream đứng. Đã qua builder check (§7 ghi các quyết định sau đó).
- **Nguồn:** review toàn bộ chat 2026-11-19, CH1 I2 ("One Push API stream per pod"). Đợt 3 gồm 3a (đã merge, K2IOT/PanGate#10), **3b** (tài liệu này), 3c (vòng Kafka theo partition).
- **Quy mô mục tiêu** (chủ dự án, 2026-11-19): 1 triệu người dùng, kênh 100 000 thành viên.

## 1. Vì sao

`chat-service/.../grpc/PushApiClient.java` hiện mở **một** stream `PushService.SendStream` mỗi pod, kênh gRPC dùng `pick_first`, mọi lần gửi đi qua cùng một khoá. Review chỉ ra bốn vấn đề:

1. Mỗi pod chat dồn vào **một pod Push API**: thêm pod Push API không tăng sức chứa.
2. Server cho tối đa 1 000 request đang xử lý mỗi stream (`max-in-flight-per-stream`, flow control thủ công), nhưng client **không xem cửa sổ đó**: phần dư nằm trong bộ đệm gRPC phía client, không giới hạn.
3. Timeout 2 s **bắt đầu trước khi server nhận** request: request còn đợi trong hàng hết giờ, fan-out gửi lại trong khi bản gốc vẫn được xử lý → bão gửi lại.
4. Chat **không gắn `idempotency_key`** cho push, nên mỗi lần gửi lại là một push mới (seq mới, `msgId` mới trong Push API). App lọc trùng theo `ChatMessage.msg_id`, nhưng mọi tầng tốn gấp đôi. (3b sửa **nguyên nhân** của bão gửi lại, mục 3, thay vì gắn khoá: §4.4.)

## 2. Mục tiêu, không làm, phản mục tiêu

**Mục tiêu:**
- Sức chứa push của một pod chat tăng theo số pod Push API.
- Không bao giờ có nhiều hơn cửa sổ của server đang chờ trên một stream; phần dư nằm trong hàng đợi có giới hạn phía chat, theo mức ưu tiên.
- Không còn bão gửi lại: request chưa được server nhận không bao giờ hết hạn vì chờ, và request đã ghi giữ chỗ tới khi server trả lời.
- Khi Push API quá tải, push ít quan trọng bị bỏ trước; tin nhắn không bao giờ bị chat bỏ.

**Không làm:** client KICK của gateway (cùng kiểu stream đơn, lượng nhỏ; để sau); đổi sang API HTTP batch; đổi contract (`PushRequest`, `PushResponse`); `idempotency_key` (§4.4); thay đổi phía Push API ngoài tuổi tối đa của kết nối (§4.2); mTLS (vẫn là TODO trước production, README).

**Phản mục tiêu:** đổi thứ tự push của **cùng một người nhận** (hoặc cùng topic); tin nhắn bị bỏ vì có trận gõ phím / online dồn dập; bộ nhớ chat tăng không giới hạn khi Push API chậm.

## 3. Cách tiếp cận đã cân nhắc

| Cách | Được | Mất |
|---|---|---|
| **A. Pool N stream, kiểm soát luồng phía client, ba mức ưu tiên** — **chọn** (chủ dự án, 2026-11-24) | Sửa cả bốn vấn đề; thêm pod Push API là thêm sức chứa; giữ thứ tự theo người nhận bằng hash | Gần như viết lại `PushApiClient`; nơi gọi phải nói mức ưu tiên |

Gắn `idempotency_key` (chủ dự án chọn **không gắn**, 2026-11-24, sau builder check):

| Cách | Được | Mất |
|---|---|---|
| **Không gắn** — **chọn** | Không tốn Redis; bão gửi lại đã hết nhờ §4.3 | Trùng hiếm khi stream đứt giữa chừng rồi gửi lại; app lọc theo `msg_id` |
| Gắn cho mọi tin nhắn | Gửi lại không trùng | ~4–5 thao tác Redis và 2 khoá sống 1 giờ mỗi người nhận (5 000 tin nhóm/s × 50 người ≈ 1,8 tỷ khoá); bài kênh lỗi khi Redis của Push API sập |
| Chỉ bài kênh và CONTROL | Ít khoá | Bài kênh phụ thuộc Redis; tin nhóm vẫn có thể trùng |
| B. API HTTP batch `POST /v1/push:batch` | Ít request với nhóm lớn; cân bằng tải HTTP sẵn | Token API cho chat; gõ phím / online không hợp lô; độ trễ gom lô; đổi nhiều nơi gọi |
| C. Sửa tối thiểu một stream | Nhỏ | Mỗi pod chat vẫn dồn vào một pod Push API; không đạt mục tiêu quy mô |

## 4. Thiết kế

### 4.1 Ba mức ưu tiên (chủ dự án, 2026-11-24)

| Mức | Push | Khi không còn chỗ |
|---|---|---|
| **MUST** (bắt buộc tới) | tin nhắn DM / nhóm (`FanOutLoop`), bài kênh (`BROADCAST`), lệnh mở / đóng / thu hồi đăng ký kênh (`ChannelSubscriptions`, `CONTROL`) | Chat không bỏ vì thiếu chỗ: xếp hàng (có giới hạn) và đi trước mọi mức khác. Hàng đầy, hoặc chờ quá `must-wait` (10 s) mà chưa ghi được (Push API sập, không stream nào mở được), thì lỗi `PushUnavailableException`: nơi gọi thử lại như hiện nay (`FanOutLoop` backoff, `GIVE_UP` 30 s rồi tua lại partition), vòng Kafka dừng đọc thêm thay vì đệm. Lệnh kênh giữ hành vi gửi hiện có (open / close cố gắng một lần; thu hồi gọi từ `Conversations` khi rời kênh); chúng chỉ được ưu tiên trong cửa sổ, không thêm vòng gửi lại. |
| **BEST_EFFORT** (cố gắng tới) | biên nhận (`ReceiptWorkerLoop`: `own_read_seq`, mốc của người khác), báo gửi bị từ chối (`PushNotifier`) | Chờ trong hàng tối đa `low-wait` (2 s), rồi bỏ (`PushDroppedException`, không phải `PushUnavailableException`). App lấy lại trạng thái qua `/v1/state`. `ReceiptWorkerLoop` coi bỏ là kết quả cuối (đếm, không gửi lại); vẫn gửi lại theo vòng như hiện nay khi server trả `UNAVAILABLE` / `RATE_LIMITED` hoặc stream đứt; `ROUND_DEADLINE` nâng lên ≥ `low-wait` + `timeout`. Biên nhận trùng vô hại (mốc chỉ tăng). |
| **DROPPABLE** (bỏ ngay được) | gõ phím (`TypingRelay`), trạng thái online (`PresenceFanOut`, ảnh chụp của `PresenceWatch`) | Không còn chỗ ngay lúc gửi thì bỏ ngay, không xếp hàng. |

- **Phần dành riêng:** BEST_EFFORT và DROPPABLE **cộng lại** chỉ dùng tối đa `low-share` (20%) cửa sổ của một stream; MUST dùng được toàn bộ cửa sổ. Một trận gõ phím không thể lấy chỗ của tin nhắn.
- **Thứ tự lấy ra khỏi hàng:** khi có chỗ, MUST trước, rồi BEST_EFFORT (trong giới hạn phần dành riêng).
- Mức ưu tiên do nơi gọi nói rõ: phương thức trừu tượng của `Pusher` (vẫn là functional interface) trở thành `send(PushRequest, Priority)`; `send(PushRequest)` là default = MUST. Các lambda hiện có (`ChatConfig.presenceWatch` wrapper `counted`, `PresenceWatchTest`, `FanOutIT`, `ChannelSubscriptionsTest`) đổi sang chữ ký mới; wrapper `counted` phải chuyển tiếp mức nó nhận, để ảnh chụp online vẫn là DROPPABLE.

### 4.2 Pool stream và cân bằng tải

- Một `ManagedChannel` tới `chat.push-api.target` (`dns:///push-api:9443`) với chính sách **`round_robin`**: gRPC mở một kết nối (subchannel) tới mỗi địa chỉ DNS trả về và trải các call qua chúng.
- Pool có `streams` (mặc định 4) stream `SendStream` dùng lâu dài, mở lười; mỗi stream là một call nên được `round_robin` đặt lên một pod Push API (với ≥ 4 pod, mỗi pod chat dùng 4 pod khác nhau; với 1 pod, cả 4 về một pod — vẫn đúng).
- **Chọn stream theo hash** của khoá đích, giống `PushGrpcService.targetKey`: `principal_id` cho MESSAGE / CONTROL, `"topic:" + topic` cho BROADCAST. Cùng người nhận luôn đi cùng stream, nên thứ tự (server giữ thứ tự theo đích trong một stream) được giữ.
- **Ràng buộc triển khai** (ghi vào README): ở Kubernetes, `push-api` phải là **headless service** (DNS trả mỗi pod một bản ghi A); với ClusterIP một IP thì `round_robin` chỉ thấy một địa chỉ (đúng nhưng không trải tải).
- **Tìm pod mới, trải lại tải:** gRPC chỉ phân giải lại DNS khi một kết nối hỏng, và stream dùng lâu dài thì không tự chuyển pod. Push API đặt **tuổi tối đa cho kết nối** (`push-api.grpc.max-connection-age`, mặc định 30 phút, ±10% ngẫu nhiên, ân hạn 30 s): hết tuổi, server gửi GOAWAY, client nối lại (phân giải lại DNS) và mở lại stream, nên pod mới được dùng trong vòng 30 phút và các stream dồn lên một pod lúc khởi động được trải lại. Đây là thay đổi duy nhất phía Push API (một dòng ở `GrpcServerLifecycle`), cũng có lợi cho client KICK của gateway.
- **Mở lại stream:** ngay khi một stream kết thúc, nếu hàng của nó còn request; lần mở thất bại liên tiếp chờ 100 ms, gấp đôi tới 5 s. Request đang chờ trả lời trên stream đã kết thúc lỗi ngay (`PushUnavailableException`); request còn trong hàng **không** lỗi mà được ghi trên stream mới (MUST vẫn bị giới hạn bởi `must-wait`).
- **Lúc Push API dừng:** `drain` của server chỉ đóng stream khi hết request đang xử lý; một client giữ cửa sổ đầy có thể làm server dừng bằng `shutdownNow` sau 10 s, và tối đa `streams × window` request lỗi cùng lúc. Chấp nhận: MUST được nơi gọi gửi lại, BEST_EFFORT / DROPPABLE mất được.

### 4.3 Kiểm soát luồng phía client

- Mỗi stream giữ số request đang chờ trả lời (`inFlight`) ≤ `window` (mặc định 1 000; **phải ≤** `max-in-flight-per-stream` của Push API). Một request chỉ được ghi lên stream khi còn chỗ; hết chỗ thì vào hàng đợi của stream đó theo mức (4.1).
- Ghi thêm cũng tôn trọng `ClientCallStreamObserver.isReady()` / `setOnReadyHandler` (bộ đệm truyền của gRPC), để không đệm ở tầng dưới.
- Hàng đợi MUST mỗi stream có giới hạn `queue-max` (mặc định 10 000); BEST_EFFORT có cùng giới hạn và hết hạn sau `low-wait`; DROPPABLE không có hàng.
- **Timeout** `chat.push-api.timeout` **bắt đầu khi request được ghi lên stream**, không phải lúc gọi `send`; mặc định nâng từ 2 s lên **8 s**, trên trường hợp xấu nhất của server (produce Kafka tới 5 s, cộng chờ khác). Thời gian chờ trong hàng đo riêng (metric) và chỉ giới hạn bởi `must-wait` / `low-wait`.
- **Một request hết giờ vẫn giữ chỗ** trong cửa sổ tới khi server trả lời nó hoặc stream kết thúc: server vẫn đếm nó, nên client không bao giờ ghi vượt cửa sổ của server. Future của nơi gọi thì lỗi đúng lúc hết giờ.
- **`request_id` trên dây do client đặt mới cho mỗi lần ghi** (ghi đè `request_id` của nơi gọi, ghép trả lời bằng id đó rồi trả `PushResponse` với `request_id` gốc). Một trả lời muộn của lần gửi trước không bao giờ hoàn tất nhầm future của lần gửi lại, và việc đếm chỗ không bị đếm hai lần.
- Trả lời đến (hoặc stream kết thúc) giải phóng một chỗ và lấy request kế tiếp ra khỏi hàng.

### 4.4 Không gắn `idempotency_key` (chủ dự án, 2026-11-24)

Chat không đặt `idempotency_key`. Bão gửi lại ở mục 1.3 hết vì hai thay đổi của §4.3 (timeout tính từ lúc ghi; request hết giờ vẫn giữ chỗ), không cần Push API dedup. Một push vẫn có thể trùng khi stream đứt sau khi server đã nhận mà chưa kịp trả lời, rồi nơi gọi gửi lại: hiếm, và app lọc trùng theo `ChatMessage.msg_id` (chat spec §6). Lý do không gắn: chi phí Redis ở Push API (bảng §3) và để bài kênh / CONTROL không phụ thuộc Redis của Push API.

### 4.4b Thứ tự đảm bảo

Thứ tự được giữ **cho cùng một đích, trong cùng một mức, giữa các lần gửi không lỗi**: các push MUST tới cùng người nhận đi cùng stream, ra khỏi hàng theo thứ tự `send`, và server giữ thứ tự đó. Không đảm bảo giữa các mức (MUST đi trước BEST_EFFORT; DROPPABLE không xếp hàng), và một push gửi lại sau khi stream đứt có thể đến sau push gọi sau nó. App đã sắp tin nhắn theo `convSeq` nên điều này không đổi gì người dùng thấy.

### 4.5 Cấu hình (`chat.push-api.*`)

| Khoá | Mặc định | |
|---|---|---|
| `target`, `plaintext` | như hiện nay | |
| `timeout` | 8s (từ 2s) | tính từ lúc ghi lên stream |
| `streams` | 4 | stream mỗi pod |
| `window` | 1 000 | ≤ `max-in-flight-per-stream` của Push API |
| `low-share` | 0.2 | phần cửa sổ tối đa cho BEST_EFFORT + DROPPABLE |
| `low-wait` | 2s | BEST_EFFORT chờ tối đa |
| `queue-max` | 10 000 | mỗi stream, mỗi mức có hàng |
| `must-wait` | 10s | MUST chờ trong hàng tối đa (Push API sập, không stream nào mở được) |
| Push API `grpc.max-connection-age` | 30m (±10%), ân hạn 30s | GOAWAY để client nối lại và phân giải lại DNS |

### 4.6 Metric

`chat.push.inflight{stream}`, `chat.push.queued{priority}` (gauge), `chat.push.queue.wait{priority}` (timer, từ `send` tới lúc ghi), `chat.push.dropped{priority,reason}` (`window_full`, `waited`, `queue_full`, `must_wait`), `chat.push.latency` (từ lúc ghi tới trả lời), `chat.push.stream.restarts`.

## 5. Kiểm chứng

- **Cửa sổ:** server giả (in-process) không trả lời → client ghi đúng `window` request rồi dừng; phần còn lại nằm trong hàng, không hết hạn sau `timeout`; server trả lời một → client ghi đúng một request kế tiếp. Request ghi rồi hết giờ: future lỗi nhưng client **không** ghi thêm cho tới khi server trả lời nó.
- **Trả lời muộn:** request hết giờ, nơi gọi gửi lại cùng `PushRequest`; trả lời muộn của lần đầu không hoàn tất future của lần hai (id trên dây khác nhau).
- **Push API sập:** không stream nào mở được → MUST lỗi sau `must-wait`, mở lại có backoff (không vòng chặt).
- **Ưu tiên:** cửa sổ đầy MUST → DROPPABLE bị bỏ ngay (`window_full`), BEST_EFFORT bị bỏ sau `low-wait` (`waited`); khi có chỗ, MUST ra trước BEST_EFFORT; BEST_EFFORT + DROPPABLE không bao giờ vượt `low-share` cửa sổ, kể cả khi không có MUST nào.
- **Thứ tự:** nhiều push cho cùng người nhận gửi đồng thời từ nhiều thread → server thấy đúng thứ tự gọi `send` của từng thread trên một stream; người nhận khác nhau trải qua nhiều stream.
- **Stream hỏng:** server huỷ stream → request đang chờ lỗi ngay; request trong hàng được gửi trên stream mới.
- **Trải tải:** hai server in-process sau một resolver tĩnh hai địa chỉ → cả hai nhận stream.
- **Tuổi kết nối:** IT Push API: kết nối sống quá `max-connection-age` (đặt ngắn trong test) nhận GOAWAY; client mở lại stream và push tiếp tục.
- **Biên nhận:** `ReceiptWorkerLoop` không gửi lại push bị bỏ (`PushDroppedException`), vẫn gửi lại khi server trả `UNAVAILABLE`.
- ITs hiện có (`FanOutIT`, `ReceiptWorkerIT`, `ChatE2EIT`, smoke) vẫn xanh.

## 6. Để người xây dựng tự quyết

Cấu trúc lớp (một lớp `PushStream` mỗi stream hay gộp), cấu trúc hàng đợi, cách đặt `round_robin` (`defaultLoadBalancingPolicy`), hàm hash, cách test trải tải; tên exception cho push bị bỏ (miễn phân biệt được với `PushUnavailableException`; nơi cần phân biệt là `ReceiptWorkerLoop`).

## 7. Sau builder check (2026-11-24)

Câu hỏi của chủ dự án: có gắn `idempotency_key` không → **không** (§3, §4.4). Quyết định của người viết: MUST có hạn chờ `must-wait`; request hết giờ giữ chỗ tới khi server trả lời; id trên dây mới cho mỗi lần ghi; timeout 8 s từ lúc ghi; tuổi kết nối tối đa ở Push API (thay đổi duy nhất phía Push API); mở lại stream có backoff; `ReceiptWorkerLoop` không gửi lại push bị bỏ và nâng hạn vòng; thứ tự chỉ đảm bảo theo đích và theo mức (§4.4b); `Pusher.send(r, p)` là phương thức trừu tượng; lệnh kênh không thêm vòng gửi lại; chấp nhận lỗi hàng loạt khi Push API dừng dưới tải.

## 8. Sau review cuối (2026-11-24)

Reviewer độc lập: 0 Critical, 5 Important (đã sửa, mỗi sửa có test fail trước), 6 Minor (để lại).
- **I1:** request xếp hàng trong lúc stream đang chờ mở lại (sau một lần mở hỏng) không được ghi khi hết thời gian chờ nếu không có push khác: nay `drain` hẹn mở lại khi còn hàng mà chưa có call dùng được.
- **I2:** tuổi kết nối của Push API cắt cứng mọi stream mỗi ~30 phút, làm lỗi tới `window` request đang chờ mỗi stream: nay client tự xoay call sau `stream-max-age` (5 phút ±20%), half-close call cũ để server trả lời hết rồi đóng; trả lời từ call đang xoay vẫn giải phóng chỗ; ân hạn của Push API nâng lên 10 phút.
- **I3:** call còn sống nhưng không trả lời gì giữ chỗ mãi: nay call im lặng 3 lần timeout mà mọi request trên nó đều đã hết giờ bị huỷ (giải phóng chỗ, round-robin đặt call mới chỗ khác); kênh bật keepalive 20 s.
- **I4:** `must-wait` làm lỗi MUST cả khi stream vẫn đang trả lời (chỉ chậm): nay chỉ lỗi khi stream không trả lời gì trong thời gian chờ.
- **I5:** Push API sập thật thì gRPC tạo call được rồi mới lỗi, làm lỗi ngay mọi MUST trong hàng: nay chỉ ghi lên call sau khi gRPC báo sẵn sàng lần đầu; các MUST chờ tới `must-wait`.

Để lại (Minor): timer timeout mỗi lần ghi không huỷ; tra meter mỗi lần gọi; `PushNotifier` báo push bị bỏ là "unavailable"; chưa kiểm giá trị cấu hình; chú thích lớp nói quá về giới hạn bộ đệm; chưa cảnh báo khi `timeout + low-wait` chạm `ROUND_DEADLINE` của worker biên nhận.

