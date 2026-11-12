# chat-service (Chat core) — Design Spec

- **Ngày:** 2026-11-12
- **Trạng thái:** Đã hiện thực (CH1, plan `docs/superpowers/plans/2026-11-12-chat-core-ch1.md`). Đo trên máy dev 4 vCPU (`chat-service/README.md`): 1-1 qua WebSocket p99 219 ms ở 20 tin/s, 443 ms ở 100 tin/s (trên mức 300 ms của tiêu chí); nhóm 500 p99 276 ms; kênh 10 000 phiên p99 300 ms.
- **Các spec liên quan:**
  - "spec gateway": `docs/superpowers/specs/2026-11-03-gateway-service-design.md`: upstream `UPSTREAM` / `UPSTREAM_ACK` (§5.5), SUBSCRIBE và giới hạn topic (§5.7), CONTROL (§6.6).
  - "spec auth": `docs/superpowers/specs/2026-11-05-auth-service-design.md`: tenant, client, connect token (`aud=pangate-gw`), JWKS.
  - "spec contracts": `docs/superpowers/specs/2026-11-10-push-contracts-design.md`: `PushRequest`, `PushRecord`, BROADCAST, CONTROL.
  - "spec push-api": `docs/superpowers/specs/2026-11-10-push-api-design.md`: `SendStream` gRPC, rate limit theo tenant, ADR 0027 (ORDERED theo pod).
- **Vị trí trong lộ trình app chat:** sub-project 1/4: **(CH1) Chat core**, (CH2) Tài khoản, (CH3) Trạng thái hội thoại (đã nhận/đã xem, chưa đọc, đang gõ, Presence), (CH4) Device Registry và Offline Delivery. Mỗi sub-project có spec, plan và hiện thực riêng.

---

## 0. Mục tiêu, phạm vi

**Mục tiêu.** Một app chat cụ thể cho người dùng cuối, chạy trên PanGate, kiểu Telegram. CH1 làm phần lõi phía server: hội thoại, gửi tin, lưu lịch sử, đồng bộ khi nối lại, và giao tin xuống client qua đường push đã có.

**Quyết định đã chốt khi brainstorm (2026-11-11 – 2026-11-12):**

| # | Câu hỏi | Quyết định |
|---|---|---|
| D1 | App chat dùng để làm gì | Một app chat cụ thể cho người dùng cuối, chạy trên PanGate |
| D2 | Loại hội thoại | Chat 1-1, nhóm nhỏ (tối đa 500), kênh lớn |
| D3 | Phạm vi lộ trình | Chỉ backend; client làm sau hoặc đội khác làm; test bằng client của devtools |
| D4 | Tính năng bản đầu | Gửi/nhận, lịch sử, đã nhận/đã xem + chưa đọc (CH3), đang gõ + online (CH3), push mobile khi offline (CH4). Ảnh/file để sau |
| D5 | Đăng nhập | Chat backend tự quản lý tài khoản (CH2) |
| D6 | Quy mô | Lớn: hàng chục triệu user; ScyllaDB ngay từ đầu |
| D7 | Mã hoá | Như Telegram: không E2EE; Secret Chat để sau |
| D8 | Đường gửi tin | Cả hai: WebSocket qua gateway là đường chính, HTTP cho bot, tích hợp và dự phòng |
| D9 | Tenant và user | Một tenant PanGate cố định `chat`; chat-service chỉ biết user (§1) |

**Tiêu chí thành công.**
- Hai client devtools mở chat 1-1 và nhóm, gửi tin qua WebSocket. Người nhận đang online thấy tin đúng thứ tự `convSeq`, p99 dưới 300 ms khi tải bình thường.
- Người nhận rớt mạng rồi nối lại, `POST /v1/sync` trả đủ các tin đã lỡ.
- Kênh: client mở kênh bằng `chat.channel.open` và nhận broadcast.
- Gửi lại cùng `clientMsgId` (qua WebSocket hay HTTP) chỉ ra một tin.
- `make smoke` có bước chat 1-1, nhóm và kênh qua gateway thật.

**Ngoài phạm vi CH1:**
- tài khoản, đăng ký, OTP, hồ sơ (CH2);
- đã nhận/đã xem, số chưa đọc theo `read_seq`, đang gõ, Presence (CH3; CH1 chỉ để sẵn cột `read_seq`);
- push mobile khi offline (CH4);
- sửa tin, xoá tin, reaction, ảnh/file, tìm kiếm, nhóm trên 500 người, token cho bot, Secret Chat, nhiều cell.

**Bất biến:**
1. Trong một hội thoại, mỗi tin có đúng một `convSeq`, tăng dần, không trùng, kể cả khi sequencer crash hay rebalance.
2. Một bộ `(conversationId, senderId, clientMsgId)` sinh ra tối đa một tin.
3. Tin chỉ được phát xuống (fan-out) sau khi đã lưu bền vào ScyllaDB.
4. chat-service không nói chuyện trực tiếp với gateway hay router; mọi tin xuống client đi qua Push API.

---

## 1. Tenant và user

**Tầng PanGate (hạ tầng).**
- Có đúng một tenant `chat` trong auth-service, với một client hệ thống `chat-backend` (OAuth2 client credentials) chỉ backend giữ.
- Mỗi user là principal `chat:<userId>`; mỗi thiết bị là một `deviceId`.
- Mọi cơ chế theo tenant của PanGate áp dụng cho toàn app: kill switch `tenant_blocked`, rate limit của Push API (trần chung), `topicsPerConn`.

**Tầng chat-service (sản phẩm).**
- Dữ liệu không có cột tenant; hội thoại, thành viên và tin gắn với `userId`.
- Rate limit và quota đặt theo user (§4.3).
- chat-service thêm hoặc bỏ tiền tố `chat:` khi nói chuyện với PanGate; principal không có tiền tố `chat:` bị từ chối.

**Xác thực ở CH1.** Chưa có dịch vụ Tài khoản: test và devenv dùng connect token do devtools tạo cho tenant `chat`. API HTTP của chat-service kiểm connect token offline qua JWKS của auth-service (`aud=pangate-gw`, `iss`, `exp` với lệch đồng hồ 30 s, jti và thiết bị chưa bị thu hồi) và lấy `userId` từ principal. CH2 có thể đổi sang token phiên của dịch vụ Tài khoản.

**Hệ quả.** Khoá một user là thu hồi thiết bị hoặc token của `chat:<userId>` qua cơ chế revoke đã có. Muốn nhiều app chat riêng về sau thì mỗi app là một tenant mới, và chat-service phải thêm lại cột tenant; giữ tiền tố `chat:` để mở đường.

---

## 2. Kiến trúc

```
client ──WS UPSTREAM(cmd, op=chat.*)──▶ gateway ──▶ c1.gw.upstream.cmd ─┐
client ──HTTP POST /v1/conversations/{id}/messages ─────────────────────┤
                                                                       ▼
                                   chat-service: Ingest (kiểm tra, giới hạn theo user)
                                                                       │ chat.commands (key conversationId)
                                                                       ▼
                                   Sequencer (một nơi ghi mỗi hội thoại, LWT, chống trùng)
                                        │ ghi ScyllaDB              │ chat.events (key conversationId)
                                        ▼                           ▼
                                   ScyllaDB ◀── Query API       Fan-out ──gRPC SendStream──▶ Push API ─▶ router ─▶ gateway ─▶ client
```

| Khối | Trách nhiệm |
|---|---|
| `Ingest` | Đọc `c1.gw.upstream.cmd` (lọc `headers.op` bắt đầu bằng `chat.`) và nhận `POST .../messages`; kiểm tra lệnh, thành viên, giới hạn theo user và theo hội thoại; ghi `chat.commands` |
| `Sequencer` | Đọc `chat.commands`, gom lô theo hội thoại; chống trùng; cấp `convSeq` bằng LWT; ghi tin; ghi `chat.events`; trả kết quả cho đường HTTP qua Redis |
| `FanOut` | Đọc `chat.events`; gửi MESSAGE tới từng thành viên (1-1, nhóm) hoặc BROADCAST theo topic (kênh) qua Push API; cập nhật `user_convs.last_seq` của thành viên nhóm |
| `ChannelSubscriptions` | Xử lý `chat.channel.open` / `close`: kiểm quyền, gửi CONTROL `subscribe` / `unsubscribe` qua Push API |
| `QueryApi` | HTTP: danh sách hội thoại, lịch sử, đồng bộ, tạo hội thoại, mở chat 1-1, thành viên |
| `ChatStore` | Truy cập ScyllaDB (§5) |

Lõi (`Ingest`, `Sequencer`, `FanOut`, kiểm tra lệnh, giới hạn) không import Spring và test được không cần Spring, như Push API.

**Không đổi:** gateway, router, auth, Push API. Chỉ thêm cấu hình: SPIFFE ID của chat-service vào `push-api.grpc.allowed-spiffe`, và `TenantCfg` của tenant `chat` (§8).

---

## 3. Contracts (`platform-contracts`, chỉ thêm)

`platform-contracts/proto/pangate/v1/chat.proto` (package `pangate.v1`), `buf breaking` phải xanh:

```proto
message ChatSend {                    // client → server, payload của UPSTREAM(type=cmd, headers.op=chat.send)
  string conversation_id = 1;
  string client_msg_id = 2;           // ULID do client tạo; khoá chống trùng
  string text = 3;                    // UTF-8, 1..4096 byte
  string reply_to_msg_id = 4;         // tuỳ chọn
  repeated string mention_user_ids = 5;
}
message ChannelOpen  { string conversation_id = 1; }   // op=chat.channel.open
message ChannelClose { string conversation_id = 1; }   // op=chat.channel.close

message ChatMessage {                 // server → client, payload của PUSH (headers.type=chat.message)
  string conversation_id = 1;
  string msg_id = 2;                  // ULID do sequencer cấp
  uint64 conv_seq = 3;
  string sender_id = 4;
  string client_msg_id = 5;           // để thiết bị gửi thay bản tạm
  string text = 6;
  string reply_to_msg_id = 7;
  repeated string mention_user_ids = 8;
  int64 sent_at_ms = 9;
}
message ChatRejected {                // server → các thiết bị của người gửi (headers.type=chat.rejected)
  string conversation_id = 1;
  string client_msg_id = 2;
  string reason = 3;                  // not_member | not_found | too_long | rate_limited | invalid
}

message ChatCommand {                 // chat.commands, key conversation_id
  string command_id = 1;              // ULID
  string sender_id = 2;               // userId, không có tiền tố
  string sender_device_id = 3;
  int64 received_at_ms = 4;
  oneof op { ChatSend send = 10; }
  string reply_to = 20;               // "http:<pod>" khi pod HTTP chờ kết quả; rỗng với WebSocket
}
message ChatEvent {                   // chat.events, key conversation_id
  ChatMessage message = 1;
  ConversationKind kind = 2;          // DIRECT | GROUP | CHANNEL
}
enum ConversationKind { CONVERSATION_KIND_UNSPECIFIED = 0; DIRECT = 1; GROUP = 2; CHANNEL = 3; }
```

Testkit thêm topic, key và header của `chat.commands` / `chat.events`, cùng các case hợp lệ và không hợp lệ của `ChatSend`.

---

## 4. Gửi tin

### 4.1 Đường WebSocket

1. Client gửi `UPSTREAM` với `headers.type=cmd`, `headers.op=chat.send`, payload `ChatSend`.
2. Gateway ghi `c1.gw.upstream.cmd` rồi trả `UPSTREAM_ACK` (spec gateway §5.5). Với client: "đã gửi lên server", chưa phải đã vào hội thoại. Kafka lỗi thì gateway trả `ERROR 4500`; client gửi lại với cùng `clientMsgId`.
3. `Ingest` lấy `principal` và `device_id` từ `UpstreamRecord`, bỏ tiền tố `chat:`, kiểm tra §4.3 rồi ghi `ChatCommand` vào `chat.commands`. Lỗi kiểm tra thì gửi `ChatRejected` về các thiết bị của người gửi (MESSAGE EPHEMERAL qua Push API) và không ghi lệnh.
4. `Sequencer` xử lý (§4.4).
5. `FanOut` gửi `ChatMessage` tới mọi thành viên, kể cả các thiết bị của người gửi; thiết bị gửi đối chiếu `clientMsgId` và thay bản tạm. Đó là "đã gửi".

### 4.2 Đường HTTP

`POST /v1/conversations/{id}/messages`, header `Idempotency-Key` bắt buộc (dùng làm `clientMsgId`), body `{text, replyToMsgId?, mentionUserIds?}`.
- Cùng bộ kiểm tra §4.3; lỗi trả `400` / `403` / `404` / `429` (problem+json).
- Lệnh ghi `chat.commands` với `reply_to=http:<pod>`. Pod chờ kết quả tối đa 3 s trên Redis (key `sendres:{conv}:{sender}:{clientMsgId}` cùng pub/sub `sendres:<pod>`).
- Có kết quả: `201 {msgId, convSeq, sentAt}`. Quá 3 s, hoặc Redis không dùng được: `202 {clientMsgId}`; gọi lại cùng key trả đúng kết quả khi đã có.

### 4.3 Kiểm tra lệnh (`Ingest`)

| Luật | Lỗi |
|---|---|
| Principal có tiền tố `chat:`; `conversationId` là ULID | `invalid` |
| `clientMsgId` là ULID; `text` 1..4096 byte UTF-8; tối đa 50 `mention_user_ids` | `invalid` / `too_long` |
| Hội thoại tồn tại | `not_found` |
| Người gửi là thành viên; với kênh, người gửi có vai trò `owner` hoặc `admin` | `not_member` |
| Giới hạn theo user: 20 tin/giây, burst 40 | `rate_limited` |
| Giới hạn theo hội thoại: 50 tin/giây, burst 100 | `rate_limited` |

Giới hạn là token bucket trong bộ nhớ mỗi pod. Lệnh của một user có thể tới nhiều pod (WebSocket theo partition của principal, HTTP qua load balancer), nên giới hạn thực tế là giới hạn × số pod, như Push API (ADR 0026). Thành viên đọc qua cache Redis (TTL 60 s), hụt cache thì đọc ScyllaDB.

### 4.4 Sequencer

Đọc `chat.commands` theo consumer group `chat-sequencer`. Mỗi lần poll, gom lệnh theo hội thoại, giữ thứ tự offset. Với mỗi hội thoại:

1. **Chống trùng.** Với từng lệnh, tra `send_dedup` (Redis trước, ScyllaDB sau). Đã có thì dùng lại `msgId` / `convSeq` cũ: không cấp số mới, nhưng vẫn ghi lại `ChatEvent` của tin đó (tầng dưới dedup theo `msg_id`).
2. **Cấp `convSeq`.** Với n lệnh mới: `UPDATE conversations SET last_seq = :known + n WHERE conv_id = :id IF last_seq = :known` (LWT), giữ chỗ `known+1 .. known+n`. `known` lấy từ bộ nhớ, đọc lại từ ScyllaDB khi thiếu. LWT thất bại nghĩa là có nơi ghi khác (rebalance): đọc lại `last_seq`, huỷ lô, `seek` về đầu lô và để Kafka quyết định partition thuộc ai.
3. **Ghi tin.** Ghi `messages` (§5), `send_dedup` (ScyllaDB, TTL 24 h) và Redis (TTL 24 h). `msgId` là ULID; `sent_at_ms` là thời điểm sequencer xử lý.
4. **Ghi `chat.events`** (producer idempotent, `acks=all`). Lệnh HTTP: ghi kết quả vào Redis và publish lên `sendres:<pod>`.
5. **Commit offset** sau khi mọi bước trên của cả lô xong.

Crash giữa bước 3 và 5: khi đọc lại, bước 1 tìm thấy tin đã ghi và phát lại sự kiện; không có `convSeq` mới.

---

## 5. Lưu trữ

**ScyllaDB**, keyspace `chat`, `NetworkTopologyStrategy` (devenv: một node, RF 1):

| Bảng | Khoá phân vùng → sắp xếp | Cột chính |
|---|---|---|
| `messages` | `(conv_id, bucket)` → `conv_seq DESC` | `msg_id`, `sender_id`, `client_msg_id`, `text`, `reply_to`, `mentions`, `sent_at` |
| `conversations` | `conv_id` | `kind`, `title`, `created_by`, `created_at`, `last_seq` (hàng rào LWT), `last_msg_at`, `member_count` |
| `members` | `conv_id` → `user_id` | `role` (`owner` / `admin` / `member`), `joined_at` |
| `user_convs` | `user_id` → `conv_id` | `kind`, `last_seq`, `read_seq` (CH3), `last_msg_at`, `muted` |
| `dm_index` | `(user_lo, user_hi)` | `conv_id` (một chat 1-1 duy nhất cho mỗi cặp) |
| `send_dedup` | `(conv_id, sender_id, client_msg_id)` | `msg_id`, `conv_seq`, `sent_at`; TTL 24 h |

- `bucket = conv_seq / 10 000`: partition của kênh lớn không phình vô hạn; đọc lịch sử đi qua tối đa hai bucket mỗi trang.
- `user_convs` của thành viên 1-1 và nhóm được `FanOut` cập nhật mỗi tin (ghi thường, không LWT; `last_seq` chỉ ghi khi lớn hơn, nhờ ghi kèm timestamp = `conv_seq`). Với kênh thì **không** cập nhật cho từng người theo dõi; khi đồng bộ, client so `conversations.last_seq` với con trỏ của mình.
- Schema được tạo bằng migration chạy lúc khởi động (bảng `schema_version`), idempotent.

**Redis:** cache thành viên (`members:{conv}`), chống trùng nhanh (`dedup:{conv}:{sender}:{clientMsgId}`), kết quả cho HTTP (`sendres:*`).

**Thời gian lưu:** giữ tin vô hạn ở CH1.

---

## 6. Fan-out

`FanOut` đọc `chat.events` theo group `chat-fanout`; mỗi hội thoại thuộc một partition nên tin của một hội thoại được xử lý tuần tự, đúng thứ tự `convSeq`. Gọi Push API bằng gRPC `SendStream` (SPIFFE ID của chat-service trong allowlist; plaintext ở local/dev).

**Chat 1-1 và nhóm (tối đa 500 thành viên).** Mỗi thành viên một `PushRequest`: `kind=MESSAGE`, `class=RELIABLE`, `principal_id=chat:<userId>`, `payload=ChatMessage`, `headers.type=chat.message`, không đặt `idempotency_key` (500 key Redis mỗi tin nhóm là quá đắt ở quy mô này). Router giao tới mọi thiết bị đang online; người offline đi vào `cN.push.offline` (CH4 sẽ đọc). Nhóm 500 người là 500 push mỗi tin.

**Kênh.** Một `PushRequest`: `kind=BROADCAST`, `topic=chat:chan:<convId>`, `class=EPHEMERAL`. Người không online lấy tin qua `/v1/sync`.

**Mở và đóng kênh.** `op=chat.channel.open` (WebSocket): kiểm quyền (kênh công khai, hoặc là thành viên kênh riêng tư) rồi gửi `CONTROL subscribe {topic}` cho `chat:<userId>`; `chat.channel.close` gửi `unsubscribe`. Gateway chỉ kiểm tra topic đúng tenant và còn trong `topicsPerConn` (spec gateway §6.6), nên quyền nằm ở chat-service. Client giữ tối đa khoảng 50 kênh mở; các kênh khác cập nhật qua đồng bộ. Sau khi nối lại, client gửi lại `chat.channel.open` cho các kênh đang mở.

**Chống trùng ở client.** Fan-out thử lại hoặc sequencer phát lại sự kiện có thể làm cùng một tin xuống hai lần với `msg_id` của Push API khác nhau; client dedup theo `ChatMessage.msg_id` (và `conv_seq`), không theo `msg_id` của frame.

**Lỗi khi gửi xuống.** Push API trả lỗi tạm (`UNAVAILABLE`, `RATE_LIMITED`) thì thử lại với backoff 100 ms → 5 s, không commit offset. Lỗi cố định (`INVALID`) thì đếm metric và bỏ qua mục đó.

---

## 7. API đọc và quản lý hội thoại

Mọi endpoint dùng `Authorization: Bearer <connect token>` (§1); user lấy từ token.

| Endpoint | Hành vi |
|---|---|
| `GET /v1/conversations?cursor=&limit=` | Hội thoại của tôi, sắp theo `last_msg_at`, kèm `lastSeq`; tối đa 100 |
| `GET /v1/conversations/{id}` | Thông tin hội thoại; phải là thành viên (kênh công khai thì không cần) |
| `GET /v1/conversations/{id}/messages?before=&after=&limit=` | Lịch sử theo `convSeq`, tối đa 100 mỗi trang |
| `POST /v1/sync` body `{conversations:[{id, lastSeq}], limitPerConversation?}` | Với mỗi hội thoại: tin có `convSeq > lastSeq`, tối đa 100 (mặc định 50); quá thì `{tooMany: true, lastSeq}` để client tải trang mới nhất. Tối đa 100 hội thoại mỗi lần |
| `POST /v1/conversations` body `{kind: group|channel, title, memberIds?, public?}` | Tạo nhóm (tối đa 500 thành viên) hoặc kênh; người tạo là `owner` |
| `POST /v1/dm/{userId}` | Mở hoặc lấy chat 1-1 với user đó (`dm_index`, LWT `IF NOT EXISTS`) |
| `POST /v1/conversations/{id}/members` / `DELETE .../members/{userId}` | Thêm/bớt thành viên; cần `owner` hoặc `admin`; tự rời thì ai cũng được |
| `POST /v1/conversations/{id}/join` | Theo dõi kênh công khai |

Lỗi trả `application/problem+json` với `code` ổn định: `invalid_request`, `not_found`, `forbidden`, `rate_limited`, `unavailable`, `invalid_token`.

---

## 8. Xử lý lỗi và vận hành

**Topic Kafka mới** (tạo bởi `kafka-init` ở devenv): `chat.commands` (64 partition), `chat.events` (64), `chat.commands.dlq` (8). Không có tiền tố cell, vì chat-service ở CH1 chạy một vùng.

| Sự cố | Hành vi |
|---|---|
| Kafka chậm hoặc sập | WebSocket: gateway trả `ERROR 4500`, client gửi lại cùng `clientMsgId`. HTTP: `503` + `Retry-After`. Sequencer và fan-out không commit |
| ScyllaDB chậm hoặc sập (timeout 1 s) | Sequencer dừng ở lô hiện tại, thử lại với backoff, lag tăng. Ingest vẫn nhận lệnh. API đọc trả `503`; `/ready` 503 sau 10 s lỗi liên tục |
| Redis sập | Chống trùng chỉ dùng ScyllaDB; cache thành viên đọc thẳng ScyllaDB; HTTP trả `202` ngay |
| Push API không gọi được | Fan-out thử lại, không commit; tin đã lưu nên không mất |
| Rebalance, hai sequencer cùng giữ một partition | LWT thất bại ở bên cũ: huỷ lô, đọc lại `last_seq`; chống trùng giữ một số cho mỗi tin |
| Hội thoại quá nóng | Giới hạn 50 tin/giây mỗi hội thoại (§4.3); sequencer gom lô nên vẫn đạt vài trăm tin/giây |
| Lệnh hỏng | Bỏ qua, đếm, ghi `chat.commands.dlq`; không chặn partition |
| SIGTERM | Dừng nhận lệnh mới; xong lô đang chạy (tối đa 10 s); commit; đóng kết nối |

**Cấu hình** (`chat.yml`, theo khuôn `push-api.yml`): cell `c1`; Kafka; ScyllaDB (contact points, keyspace, timeout 1 s); Redis; auth JWKS và issuer; Push API target và TLS; giới hạn theo user và theo hội thoại; tối đa thành viên nhóm (500); cổng HTTP / quản trị.

| Profile | Kafka / ScyllaDB / Redis | Push API | HTTP / quản trị |
|---|---|---|---|
| `local` | `localhost` | `localhost:9443` plaintext | 8086 / 8087 |
| `dev` | `kafka:29092` / `scylla:9042` / `redis:6379` | `dns:///push-api:9443` plaintext | 8080 / 8081 (host 8086 / 8087) |
| `prod` | Theo môi trường | mTLS | 8080 / 8081 |

**Cấu hình PanGate cần thêm:** `push-api.grpc.allowed-spiffe` thêm SPIFFE ID của chat-service; `TenantCfg` tenant `chat`: `push_rate` / `push_burst` đủ cho fan-out nhóm, `topicsPerConn` ≥ 64.

**Metrics:**
- `chat.send.latency` (Ingest nhận → `chat.events` ghi xong);
- `chat.sequencer.lag`, `chat.fanout.lag` (record và giây);
- `chat.send.rejected{reason}`, `chat.fanout.push{result}`, `chat.lwt.conflicts`, `chat.dedup.hits`;
- `chat.api.requests{route, code}`.

**Devenv:** thêm ScyllaDB một node vào `docker-compose.yml`; service `chat-service` (profile `app`, phụ thuộc kafka-init, redis, scylla, auth, push-api); seed tenant `chat` và client `chat-backend` trong auth; `make chat-local`.

---

## 9. Kiểm thử

| Tầng | Nội dung |
|---|---|
| Unit (lõi không Spring) | Kiểm tra lệnh §4.3 theo các case của testkit; giới hạn theo user và hội thoại; sequencer: gom lô, cấp `convSeq`, chống trùng, LWT xung đột (store giả); chọn cách fan-out theo loại hội thoại; logic `/v1/sync` (`tooMany`) |
| IT (Testcontainers ScyllaDB, Kafka, Redis) | Hai sequencer giành một partition không cấp trùng `convSeq`; crash sau khi ghi tin, trước khi commit, vẫn chỉ một tin; phân trang lịch sử qua nhiều bucket; Redis sập thì chống trùng qua ScyllaDB; migration chạy lại không lỗi |
| E2E IT (chat-service, Push API, router, 2 gateway thật, client devtools) | 1-1 qua WebSocket đúng `convSeq`; nhóm 3 người, kể cả thiết bị thứ hai của người gửi; kênh mở bằng `chat.channel.open` rồi nhận broadcast; B rớt, A gửi 5 tin, B nối lại và `/v1/sync` trả đủ 5; HTTP có chờ kết quả (`201`) và quá hạn (`202`); gửi lại cùng `clientMsgId` chỉ một tin |
| Devenv | `make smoke` thêm `CHAT 1-1`, `CHAT group`, `CHAT channel` qua gateway thật |
| Đo đạc | Nhóm 500 người; kênh 10 000 phiên đang xem; p99 gửi tới nhận |

---

## 10. Thứ tự hiện thực (plan CH1)

1. Contracts `chat.proto` và testkit (chỉ thêm).
2. Khung `chat-service`: cấu hình, migration ScyllaDB, devenv (ScyllaDB, topic, seed tenant `chat`).
3. `ChatStore` (ScyllaDB) và Redis, kèm IT.
4. `Ingest`: upstream `cmd`, đường gửi HTTP, kiểm tra lệnh, giới hạn.
5. `Sequencer`: LWT, chống trùng, `chat.events`, kết quả cho HTTP, kèm IT hàng rào và crash.
6. `FanOut`: gửi từng thành viên và broadcast kênh qua Push API.
7. `QueryApi`: hội thoại, lịch sử, đồng bộ, tạo, 1-1, thành viên, join kênh.
8. `ChannelSubscriptions`: `chat.channel.open` / `close`.
9. E2E IT, smoke, README, ADR, đo đạc.

## 11. ADR dự kiến

| # | Quyết định |
|---|---|
| 0028 | App chat là một tenant PanGate cố định; chat-service chỉ biết user |
| 0029 | Gửi tin qua WebSocket (`UPSTREAM cmd`, `op=chat.*`) là đường chính, HTTP là đường phụ; không sửa gateway |
| 0030 | Một nơi ghi mỗi hội thoại: `chat.commands` theo `conversationId`, `convSeq` cấp bằng LWT làm hàng rào |
| 0031 | Fan-out: MESSAGE theo thành viên cho 1-1 và nhóm ≤ 500; BROADCAST theo topic cho kênh, subscribe do server quyết qua CONTROL |
| 0032 | ScyllaDB cho tin và hội thoại; `messages` theo `(conv_id, bucket)` |
