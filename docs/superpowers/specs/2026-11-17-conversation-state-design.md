# Trạng thái hội thoại (CH3): đã nhận / đã xem, chưa đọc, đang gõ, online — thiết kế

- **Trạng thái:** đã hiện thực (plan `docs/superpowers/plans/2026-11-17-conversation-state-ch3.md`)
- **Ngày:** 2026-11-17
- **Vị trí trong lộ trình app chat:** sub-project 3/4. (CH1) Chat core và (CH2) Tài khoản đã xong; (CH4) Device Registry và Offline Delivery làm sau.
- **Dựa trên:** spec chat core `2026-11-12-chat-core-design.md` (gọi tắt "CH1 §n"), spec tài khoản `2026-11-16-account-service-design.md`, contracts push `2026-11-10-push-contracts-design.md`.

---

## 0. Mục tiêu, phạm vi

**Mục tiêu.** Thêm vào app chat những trạng thái người dùng thấy hằng ngày quanh một hội thoại: tin đã tới máy người nhận chưa, họ đã xem chưa, mỗi hội thoại còn bao nhiêu tin chưa đọc, ai đang gõ, ai đang online. Cùng lúc chốt **hợp đồng đồng bộ phía client**: client biết vị trí của mình trong từng hội thoại, phát hiện tin bị thiếu và lấy bù đúng phần thiếu (việc đã để sau ở CH1).

**Quyết định đã chốt khi brainstorm (2026-11-17):**

| # | Câu hỏi | Quyết định |
|---|---|---|
| S1 | Ai thấy một người online / lần cuối hoạt động | Chỉ người đang chung ít nhất một hội thoại (DM hoặc nhóm) với họ. Người lạ không thấy. Bổ sung sau review (chủ dự án duyệt): DM hay nhóm chỉ tính khi người được xem đã gửi ít nhất một tin trong đó (ai tạo nhóm hay ai thêm ai đều không chứng minh gì: một tài khoản phụ tạo được nhóm) |
| S2 | Hiển thị online | "Đang hoạt động" hoặc "Hoạt động N phút trước"; quá 7 ngày chỉ "Hoạt động gần đây", không lộ giờ |
| S3 | Đã nhận / đã xem | DM: Đã gửi → Đã nhận → Đã xem từng tin. Nhóm ≤ 50 người: danh sách ai đã xem. Nhóm 51–500: chỉ "N người đã xem". Kênh: không có |
| S4 | Tắt được | Tắt online thì không ai thấy mình và mình không thấy ai. Tắt "đã xem" cũng có qua có lại. Số chưa đọc của mình vẫn đúng |
| S5 | Đang gõ | DM và nhóm; không có ở kênh; nhóm đông client gộp "A, B và N người khác đang gõ…" |
| S6 | Chưa đọc | Riêng từng người, đồng bộ giữa các thiết bị của người đó |
| S7 | Cách xây | Hướng A: mọi thứ trong chat-service; online chỉ đẩy cho người mà client đang xem (client đăng ký danh sách) |

**Tiêu chí thành công.**
- DM: A gửi, B đang online → A thấy "Đã nhận" trong 1 s; B mở hội thoại → A thấy "Đã xem" trong 1 s.
- Nhóm 3 người: người gửi thấy danh sách ai đã xem; nhóm 100 người: `GET .../receipts` trả đúng số người đã xem.
- Đọc trên thiết bị 1 → thiết bị 2 của cùng người thấy số chưa đọc về 0 (đang online: trong 1 s; vừa nối lại: sau một lần gọi `/v1/state`).
- B gõ trong DM → A thấy "đang gõ" trong 1 s và tự tắt sau 6 s nếu B ngừng.
- B nối hoặc ngắt kết nối → A (đang xem B) thấy B online/offline; B mất mạng chập chờn dưới 5 s không làm A thấy nhấp nháy.
- Người không chung hội thoại nào với B không lấy được trạng thái online của B, qua cả WebSocket lẫn HTTP.
- Client rớt mạng giữa lúc nhận tin, nối lại theo hợp đồng §7 thì không thiếu và không trùng tin nào.
- `make smoke` có bước `CHAT receipts`, `CHAT typing`, `CHAT presence`.

**Ngoài phạm vi CH3:**
- push mobile khi offline, số chưa đọc trên icon app (badge) qua APNs/FCM (CH4);
- đếm riêng số lần được @nhắc, tắt thông báo theo hội thoại (cột `muted` có sẵn nhưng không dùng ở CH3);
- giờ xem chính xác của từng người ("Lan đã xem lúc 10:02");
- đã nhận / đã xem cho từng thiết bị;
- online cho kênh, "đang gõ" ở kênh;
- trạng thái tuỳ chỉnh ("Đang họp"), ẩn online với từng người cụ thể;
- nhiều cell.

**Phản mục tiêu** (có chạy cũng là hỏng):
- Một người lạ dò được ai đang online hoặc giờ hoạt động của bất kỳ ai.
- Bật "tắt đã xem" mà người khác vẫn suy ra được mình đã xem.
- Mạng di động chập chờn sinh bão push online/offline tới những người đang xem.
- Số chưa đọc nhảy ngược (đã về 0 lại hiện lại) vì gói tin tới không đúng thứ tự.

**Bất biến:**
1. Mốc `delivered_seq` và `read_seq` của một người trong một hội thoại chỉ tăng, không bao giờ giảm, bất kể thứ tự lệnh tới.
2. `read_seq ≤ last_seq` của hội thoại; `delivered_seq ≥ read_seq` (đọc rồi thì coi như đã nhận).
3. Mọi trạng thái của người khác (online, đã xem, đang gõ) chỉ tới một người khi họ đang chung hội thoại và luật tắt §6 cho phép, kiểm ở server.
4. Không bất biến nào của CH1 bị đổi: tin, `convSeq`, fan-out tin giữ nguyên.

---

## 1. Kiến trúc

```
client ──UPSTREAM op=chat.delivered|chat.read──▶ gateway ─▶ c1.gw.upstream.cmd ─▶ Ingest ──chat.receipts (key conv)──▶ ReceiptWorker
client ──HTTP POST /v1/conversations/{id}/read ─────────────────────────────────▶ Ingest ─┘          │ ghi ScyllaDB (mốc)
                                                                                                    │ gom 0,5–2 s / hội thoại
                                                                                                    ▼
client ──UPSTREAM op=chat.typing──▶ gateway ─▶ upstream.cmd ─▶ Ingest ─────────(trực tiếp)──▶ Push API (EPHEMERAL) ─▶ client
client ──UPSTREAM op=chat.presence.watch──▶ … ─▶ Ingest ─▶ Redis watchers:*  ──▶ ảnh chụp ngay ─▶ Push API
gateway ─▶ c1.gw.lifecycle ─▶ PresenceTracker ─▶ Redis pres:* ─▶ thay đổi (sau 5 s ân hạn) ─▶ PresenceFanOut ─▶ Push API
```

| Khối (mới trong chat-service) | Trách nhiệm |
|---|---|
| `Ingest` (mở rộng) | Thêm các op `chat.delivered`, `chat.read`, `chat.typing`, `chat.presence.watch`; kiểm tra §3.3; mốc ghi vào `chat.receipts`; typing và watch xử lý ngay |
| `ReceiptWorker` | Đọc `chat.receipts` (group `chat-receipts`); ghi mốc vào ScyllaDB chỉ khi tăng; gom theo hội thoại rồi đẩy cập nhật (§4) |
| `TypingRelay` | Giới hạn tần suất và đẩy `chat.typing` tới thành viên đang online (§5) |
| `PresenceTracker` | Đọc `c1.gw.lifecycle` (group `chat-presence`); giữ tập kết nối mỗi user trong Redis; phát hiện online ↔ offline với ân hạn 5 s (§6) |
| `PresenceWatch` | Lưu danh sách "đang xem ai" của từng thiết bị; kiểm luật chung hội thoại; trả ảnh chụp; `PresenceFanOut` đẩy thay đổi cho người đang xem |
| `StateApi` | HTTP: `/v1/state`, `/v1/conversations/{id}/receipts`, `/v1/presence`, `/v1/me/privacy` (§7, §8) |

Như CH1, lõi không import Spring. Mọi thứ xuống client đi qua Push API, lớp `EPHEMERAL` (không vào hàng offline: trạng thái cũ không có giá trị; client nối lại thì lấy bằng `/v1/state`).

**Không dùng `DeliveredRecord` của router** (`receipt=true`): nó tính cho từng tin và từng thiết bị (nhóm 500 người = 500 bản ghi Kafka mỗi tin), trong khi app chỉ cần mốc theo người. Router, gateway, auth, Push API không đổi.

---

## 2. Contracts (`chat.proto`, chỉ thêm; `buf breaking` xanh)

```proto
// client → server (UPSTREAM type=cmd)
message ChatMark {                    // op=chat.delivered hoặc op=chat.read
  string conversation_id = 1;
  uint64 up_to_seq = 2;               // đã nhận / đã xem mọi tin có convSeq ≤ up_to_seq
}
message ChatTyping { string conversation_id = 1; }      // op=chat.typing; gửi lại mỗi 3 s khi còn gõ
message PresenceWatch {               // op=chat.presence.watch; thay toàn bộ danh sách của thiết bị này
  message Entry { string user_id = 1; string conversation_id = 2; }  // hội thoại chung để chứng minh quyền xem
  repeated Entry entries = 1;         // tối đa 200; rỗng = thôi xem
}

// server → client (PUSH, EPHEMERAL)
message ReceiptUpdate {               // headers.type=chat.receipts
  string conversation_id = 1;
  message Mark { string user_id = 1; uint64 delivered_seq = 2; uint64 read_seq = 3; }
  repeated Mark marks = 2;            // DM và nhóm ≤ 50: mốc của từng người đã thay đổi
  uint64 own_read_seq = 3;            // gửi tới thiết bị khác của chính người đọc: đồng bộ chưa đọc
}
message TypingUpdate {                // headers.type=chat.typing
  string conversation_id = 1;
  string user_id = 2;
  uint32 ttl_ms = 3;                  // client tắt "đang gõ" sau ttl_ms nếu không có cập nhật mới (6000)
}
message PresenceUpdate {              // headers.type=chat.presence
  message State {
    string user_id = 1;
    bool online = 2;
    int64 last_seen_ms = 3;           // làm tròn xuống phút; 0 khi online, khi ẩn hoặc khi quá 7 ngày
    bool recently = 4;                // true = "Hoạt động gần đây" (quá 7 ngày)
    bool hidden = 5;                  // người đó tắt online, hoặc người xem tắt (có qua có lại)
  }
  repeated State states = 1;
}

// chat.receipts (key conversation_id)
message ReceiptCommand {
  string user_id = 1;
  string device_id = 2;
  string conversation_id = 3;
  uint64 delivered_seq = 4;           // 0 = không đổi
  uint64 read_seq = 5;                // 0 = không đổi
  int64 received_at_ms = 6;
}
```

Testkit thêm topic, key và header của `chat.receipts`, và các case hợp lệ / không hợp lệ của `ChatMark`, `ChatTyping`, `PresenceWatch`.

---

## 3. Đã nhận / đã xem

### 3.1 Ý nghĩa

- **Đã nhận tới N** (`delivered_seq`): ít nhất một thiết bị của người đó đã có mọi tin `convSeq ≤ N`. Client gửi `chat.delivered` khi nhận PUSH `chat.message` hoặc sau khi `/v1/sync` lấp xong khoảng trống, với **N = seq lớn nhất mà trước nó không thiếu tin nào** (không gửi khi đang có khoảng trống).
- **Đã xem tới N** (`read_seq`): người đó đã thấy tin N trên màn hình. Client gửi `chat.read` khi hội thoại đang mở và tin mới nhất hiện trên màn hình (gửi tối đa 1 lần/giây mỗi hội thoại). Đọc kéo theo đã nhận: `delivered_seq = max(delivered_seq, read_seq)`.
- Tin của chính mình: người gửi được coi đã nhận và đã xem tin của họ. **`FanOut` (CH1 §6) đổi một chỗ:** với mỗi tin, ghi luôn `delivered_seq` và `read_seq` của người gửi bằng `conv_seq` (`USING TIMESTAMP`, ở cả `members` và `user_convs`, mọi loại hội thoại kể cả kênh), để `unread` của người gửi không tăng vì tin của chính họ. Khi tính "ai đã xem" tin N, bỏ qua người gửi tin N.
- Hiển thị (việc của client): tin N trong DM là "Đã xem" khi mốc đọc của người kia ≥ N, "Đã nhận" khi mốc nhận ≥ N, còn lại "Đã gửi". Nhóm ≤ 50: dưới tin N hiện ảnh những người có `read_seq ≥ N`. Nhóm 51–500: "N người đã xem" lấy theo yêu cầu (§3.4). Ở nhóm, CH3 **chỉ có "đã xem"**, không có "đã nhận" (quyết định của tôi: danh sách "đã nhận" trong nhóm ít người xem mà tốn gấp đôi push).

### 3.2 Lưu trữ (ScyllaDB, migration `V3`)

| Bảng | Thay đổi |
|---|---|
| `members` (`conv_id` → `user_id`) | Thêm `delivered_seq bigint`, `read_seq bigint`: đọc mốc của cả hội thoại bằng một partition |
| `user_convs` (`user_id` → `conv_id`) | Dùng cột `read_seq` có sẵn; thêm `delivered_seq bigint`: số chưa đọc của tôi |
| `user_settings` (mới, `user_id`) | `hide_presence boolean`, `hide_read boolean`, `updated_at` (§8) |
| `last_seen` (mới, `user_id`) | `last_seen_ms bigint` — bản bền của lần cuối hoạt động (§6.2) |

**Ghi mốc chỉ tăng, không LWT:** mỗi lần ghi một mốc dùng `USING TIMESTAMP <giá trị mốc>` cho đúng cột đó (cùng kỹ thuật `last_seq` của CH1 §5). Ghi một mốc nhỏ hơn mốc đã có thì ScyllaDB tự bỏ, nên lệnh tới trễ, trùng hay sai thứ tự đều không làm mốc lùi (bất biến 1). Mỗi cột chỉ được ghi theo cách này; không chỗ nào khác ghi các cột mốc.

**Chưa đọc** = `max(0, last_seq − read_seq)` của tôi, chặn trên 999 khi trả về (client hiện "999+"). DM và nhóm: `last_seq` từ `user_convs`. Kênh: `user_convs.last_seq` không được cập nhật (CH1 §5), nên lấy `conversations.last_seq`.

### 3.3 Kiểm tra lệnh (`Ingest`)

| Luật | Xử lý khi sai |
|---|---|
| `conversation_id` là ULID; `up_to_seq ≥ 1` | Bỏ, đếm `chat.receipts.rejected{reason=invalid}` |
| Người gửi là thành viên (kênh: là thành viên hoặc kênh công khai đang theo dõi) | Bỏ, `reason=not_member` |
| `up_to_seq ≤ conversations.last_seq` (đọc qua cache 5 s; quá thì kẹp về `last_seq`) | Kẹp, không bỏ |
| Giới hạn theo user: 50 mốc/giây, burst 100 (token bucket mỗi pod như CH1 §4.3) | Bỏ, `reason=rate_limited` |

Lệnh mốc không có phản hồi riêng cho client (không `ChatRejected`): mốc là gợi ý có thể gửi lại bất cứ lúc nào; mốc đúng tiếp theo sẽ ghi đè. Hợp lệ thì ghi `ReceiptCommand` vào `chat.receipts` (64 partition, key `conversation_id`).

HTTP (bot, dự phòng): `POST /v1/conversations/{id}/read` body `{upToSeq}` → cùng kiểm tra, `204`; `POST .../delivered` tương tự.

### 3.4 `ReceiptWorker`: ghi và đẩy

Đọc `chat.receipts` theo partition, nên mọi mốc của một hội thoại do một nơi xử lý.

1. **Gộp trong bộ nhớ** theo `(conv, user)`: giữ mốc lớn nhất của mỗi loại trong cửa sổ hiện tại.
2. **Xả** khi cửa sổ của hội thoại hết hạn: DM 0,5 s; nhóm 2 s; kênh xả ngay (không đẩy cho ai, chỉ ghi). Mỗi lần xả:
   - ghi ScyllaDB: `members(conv,user)` và `user_convs(user,conv)` với `USING TIMESTAMP`;
   - **đồng bộ thiết bị của chính người đọc**: nếu `read_seq` tăng, đẩy `ReceiptUpdate{own_read_seq}` tới principal `chat:<user>` (mọi thiết bị; thiết bị đã gửi lệnh tự bỏ qua);
   - **đẩy cho người khác** (DM, nhóm ≤ 50): một `ReceiptUpdate{marks}` gồm các mốc đã đổi trong cửa sổ, tới mọi thành viên khác đang được phép thấy (§8). Nhóm 51–500 và kênh: không đẩy.
3. **Commit offset** sau khi ghi ScyllaDB của các mốc tới offset đó xong. Đẩy lỗi (Push API không gọi được) thì bỏ qua sau 3 lần thử: đây là trạng thái, client sẽ lấy lại bằng `/v1/state` hoặc `GET .../receipts`. Ghi ScyllaDB lỗi thì không commit, thử lại với backoff như sequencer.

Một nhóm 50 người đang sôi nổi tốn tối đa 50 push mỗi 2 s, không phụ thuộc số người đọc.

**Tra theo yêu cầu:** `GET /v1/conversations/{id}/receipts?seq=N` (phải là thành viên; tối đa 1 lần/giây mỗi user mỗi hội thoại):
- DM / nhóm ≤ 50: `{marks: [{userId, deliveredSeq, readSeq}]}` (đã lọc theo §8);
- nhóm 51–500: `{readCount}` = số thành viên khác người gửi tin N có `read_seq ≥ N`, không tính người tắt "đã xem"; đọc partition `members` (≤ 500 dòng) và cache 5 s theo `(conv, N)`;
- kênh: `404 not_supported`.

---

## 4. Số chưa đọc và đồng bộ giữa thiết bị

- `GET /v1/conversations` (CH1 §7) trả thêm mỗi hội thoại: `readSeq`, `deliveredSeq`, `unread`. Với DM thêm `peer: {deliveredSeq, readSeq}` (đã lọc theo §8) để vẽ trạng thái tin cuối mà không cần gọi thêm.
- Thiết bị đang online nhận `ReceiptUpdate{own_read_seq}` (§3.4) khi thiết bị khác của cùng người đọc.
- Thiết bị vừa nối lại gọi `GET /v1/state` (§7.2).

---

## 5. Đang gõ

- Client gửi `chat.typing` khi người dùng bắt đầu gõ, lặp lại mỗi 3 s khi vẫn đang gõ; không gửi "ngừng gõ" (người nhận tự tắt sau `ttl_ms` = 6 s; khi tin tới, client tắt "đang gõ" của người gửi ngay).
- `Ingest` kiểm: thành viên; hội thoại không phải kênh; giới hạn 1 lệnh mỗi 2 s cho mỗi `(user, conv)` (thừa thì bỏ im lặng).
- `TypingRelay` đẩy `TypingUpdate` (EPHEMERAL, `ttl_sec=10`) tới các thành viên khác, **không đi qua Kafka** (trễ thấp, mất cũng không sao).
- Nhóm lớn: mỗi hội thoại tối đa 1 lượt đẩy typing mỗi giây trên mỗi pod (quá thì bỏ). Nhóm > 50 người chỉ đẩy tới những thành viên đang có hội thoại đó mở trên màn hình — biết được nhờ danh sách xem online (§6.3 gửi kèm `conversation_id`); nhóm ≤ 50 đẩy tới mọi thành viên. Đây là quyết định của tôi để nhóm 500 người gõ không thành 500 push mỗi lượt.
- Người tắt online (§8) vẫn gửi và nhận "đang gõ": "đang gõ" chỉ hiện trong hội thoại chung và không tiết lộ gì thêm (quyết định của tôi; Telegram làm vậy).

---

## 6. Online / lần cuối hoạt động

### 6.1 Theo dõi kết nối (`PresenceTracker`)

Đọc `c1.gw.lifecycle` (key `principal_id`, nên sự kiện của một user tới theo thứ tự trên một partition). Chỉ xét principal có tiền tố `chat:`; dedup theo `event_id` (Redis, TTL 1 h).

| Sự kiện | Redis |
|---|---|
| `CONNECTED` | `SADD pres:{user} <node>/<conn_id>`; `SADD presnode:{node}:{epoch} <user>/<conn_id>` |
| `DISCONNECTED`, `HEARTBEAT_TIMEOUT`, `KICKED` | `SREM` cả hai |
| `NODE_DOWN` (router phát; `node_id`, `epoch`) | Với mỗi phần tử của `presnode:{node}:{epoch}`: `SREM pres:{user}`; xoá tập node |

- User **online** khi `pres:{user}` không rỗng. Mỗi tập có TTL 24 h, gia hạn ở mỗi lần ghi (phòng sự kiện mất: không ai "online mãi").
- **Ân hạn 5 s:** khi tập về rỗng, ghi `presgone:{user} = at_ms` và hẹn kiểm tra sau 5 s (hàng đợi trễ trong bộ nhớ của consumer, mất khi restart thì lần kiểm tra tiếp theo hoặc TTL bù). Sau 5 s tập vẫn rỗng → **chuyển offline**: `last_seen:{user} = at_ms` (Redis + bảng `last_seen` ScyllaDB) và phát thay đổi. Nối lại trong 5 s → không có gì được phát.
- Rỗng → không rỗng (và trước đó đã offline) → **chuyển online**, phát thay đổi.
- Hai pod cùng xử lý một partition khi rebalance: các lệnh Redis là tập hợp nên lặp lại không sai; có thể phát trùng một thay đổi, client chịu được.

### 6.2 Lần cuối hoạt động

`last_seen_ms` là lúc chuyển offline, làm tròn xuống phút khi trả ra. Quá 7 ngày: `recently=true`, `last_seen_ms=0`. Online: `last_seen_ms=0`. Đọc Redis trước, hụt thì ScyllaDB.

### 6.3 Xem online của người khác (`PresenceWatch`)

- Client gửi `chat.presence.watch` với danh sách `(userId, conversationId)`, tối đa 200: những người đang hiện trên màn hình (danh sách hội thoại, hội thoại đang mở, danh sách thành viên). Mỗi lần gửi **thay toàn bộ** danh sách của thiết bị đó. Client gửi lại khi đổi màn hình, sau khi nối lại, và mỗi 4 phút.
- **Kiểm quyền từng mục:** cả người xem và `userId` là thành viên của `conversationId` (cache thành viên của CH1), và hội thoại không phải kênh. Mục sai thì bỏ lặng lẽ (không tiết lộ người đó có tồn tại không).
- **Hội thoại phải chứng minh được quan hệ** (S1, bổ sung sau review): DM hay nhóm chỉ tính khi `userId` đã gửi ít nhất một tin trong đó (`conv_marks.last_sent_seq`, fan-out ghi cùng lượt với mark của người gửi). Chặn việc người lạ tự mở DM trống, tự tạo nhóm, hay nhờ tài khoản phụ tạo nhóm để dò online. Migration `V4`.
- Lưu: `watchers:{userId}` (sorted set, phần tử `<viewer>/<device>/<conversationId>`, điểm = hạn = now + 5 phút) và `watching:{viewer}/{device}` (danh sách đang xem, để thay thế). Hết hạn thì tự rơi.
- Trả ngay **ảnh chụp**: một `PresenceUpdate` với trạng thái hiện tại của mọi mục hợp lệ, đẩy tới đúng thiết bị (`PushRequest.device_id`).
- **`PresenceFanOut`:** khi một user chuyển online/offline, đọc `watchers:{user}` (bỏ phần tử hết hạn), lọc theo §8, đẩy `PresenceUpdate` (EPHEMERAL, `ttl_sec=30`) tới từng thiết bị đang xem.
- Watch cũng cho `TypingRelay` biết ai đang mở hội thoại nào (§5): mục có `conversationId = X` của thiết bị đang mở X. Vì vậy watch của người tắt online vẫn được lưu; họ chỉ nhận `hidden=true` (§8). Client nên gửi watch cho các thành viên đang hiện khi mở một hội thoại, kể cả khi tắt online.

HTTP: `GET /v1/presence?ids=a,b,c&conversationId=X` (tối đa 100 id, cùng hội thoại X; cùng kiểm quyền) trả `{states}` — cho bot và màn hình không có WebSocket.

---

## 7. Hợp đồng đồng bộ phía client

Phần này là quy ước client phải làm theo; server bảo đảm phía mình như dưới.

### 7.1 Vị trí trong hội thoại

- Client giữ cho mỗi hội thoại `syncedSeq`: seq lớn nhất mà mọi tin `≤ syncedSeq` đều đã có trên máy.
- Nhận PUSH `chat.message` với `convSeq`:
  - `= syncedSeq + 1`: thêm tin, tăng `syncedSeq`, gửi `chat.delivered` (gộp: tối đa 1 lần/giây/hội thoại).
  - `≤ syncedSeq`: tin trùng, bỏ (dedup theo `msg_id`, CH1 §6).
  - `> syncedSeq + 1`: **có khoảng trống**. Giữ tin lại, gọi `POST /v1/sync` với `{id, lastSeq: syncedSeq}` cho hội thoại đó, ghép, rồi mới tăng `syncedSeq`.
- Server bảo đảm: `convSeq` liên tục không lỗ (CH1 bất biến 1), nên lỗ ở client luôn có nghĩa là thiếu tin, không phải tin không tồn tại. `/v1/sync` trả tin `> lastSeq` theo thứ tự, hoặc `tooMany` (CH1 §7) — khi đó client tải trang mới nhất, đặt `syncedSeq = lastSeq` mới và coi phần giữa là "lịch sử cũ" tải khi cuộn lên.

### 7.2 Sau khi nối lại

1. Mở lại kênh đang mở (CH1 §6) và gửi lại `chat.presence.watch`.
2. Gọi `GET /v1/state?since=<lần cuối nhận state, ms>`: trả các hội thoại của tôi có thay đổi sau mốc đó (tin mới, mốc của tôi, mốc của người kia trong DM), mỗi mục `{id, kind, lastSeq, readSeq, deliveredSeq, unread, peer?}`, sắp theo thay đổi mới nhất, tối đa 200 mỗi trang, kèm `cursor` và `now` (client lưu làm `since` lần sau). Không có `since` (cài mới) thì trả như `GET /v1/conversations`.
   - "Có thay đổi" dựa trên cột `user_convs.state_at` (mới, ghi cùng mọi lần ghi `last_seq`, mốc của tôi, hoặc khi ReceiptWorker ghi mốc của người kia trong DM vào dòng của tôi). Kênh: so `conversations.last_seq` với `syncedSeq` phía client như CH1.
3. Với mỗi hội thoại mà `lastSeq > syncedSeq`: gọi `/v1/sync` (gom tối đa 100 hội thoại mỗi lần, CH1 §7).
4. Cập nhật số chưa đọc từ `unread` của bước 2.

### 7.3 Tin của chính mình từ thiết bị khác

Tin gửi từ thiết bị khác của tôi tới như mọi tin (CH1 §4.1 bước 5); tôi được coi đã xem tin của chính mình, nên `unread` không tăng vì chúng.

---

## 8. Quyền riêng tư

`PUT /v1/me/privacy` body `{hidePresence?, hideRead?}` → `204`; `GET /v1/me/privacy`. Lưu `user_settings`, cache 60 s mỗi pod (thay đổi có hiệu lực trong tối đa 60 s ở pod khác; pod nhận lệnh xoá cache của mình ngay).

| Ai xem ai | Online / lần cuối | Đã xem (`read_seq`) |
|---|---|---|
| Hai người không chung hội thoại (không phải kênh) | Không bao giờ | — |
| Người kia `hidePresence` | `hidden=true` | — |
| Người xem `hidePresence` | `hidden=true` cho mọi người (có qua có lại) | — |
| Người kia `hideRead` | — | Không có mốc đọc của họ (DM: tin của mình dừng ở "Đã nhận"; nhóm: không có trong danh sách và không tính vào `readCount`) |
| Người xem `hideRead` | — | Không thấy mốc đọc của ai (có qua có lại) |

- `delivered_seq` ("Đã nhận") không bị ảnh hưởng bởi `hideRead`.
- Mốc đọc của người `hideRead` vẫn được ghi (số chưa đọc của chính họ vẫn đúng, S4); chỉ không bao giờ được trả cho người khác, kể cả gián tiếp qua `readCount` hay `peer`.
- Bật `hideRead` không xoá các mốc người khác đã thấy trước đó.

---

## 9. Xử lý lỗi và vận hành

**Topic mới:** `chat.receipts` (64 partition, retention 1 ngày). Consumer mới trên topic có sẵn: `c1.gw.lifecycle` group `chat-presence`.

| Sự cố | Hành vi |
|---|---|
| Kafka chậm | Mốc tới trễ; tin vẫn chạy. Typing không qua Kafka nên không ảnh hưởng |
| ScyllaDB chậm | ReceiptWorker dừng ở lô hiện tại, không commit, lag tăng; `/v1/state` và `receipts` trả `503` |
| Redis sập | Online: mọi người hiện offline, watch không lưu được (`chat.presence.unavailable` đếm), tự phục hồi khi Redis về và client watch lại sau tối đa 4 phút; tập kết nối được dựng lại từ sự kiện sau đó (người đang nối sẵn hiện offline tới lần nối lại kế tiếp — chấp nhận ở CH3, ghi ADR). **Sửa 2026-11-19 (F5):** sự kiện ghi Redis lỗi không bị bỏ qua mà được đọc lại (dừng 200 ms, gấp đôi tới 5 s), và chỉ được đánh dấu đã xử lý sau khi áp dụng xong; trước đây một lần ghi lỗi khi ngắt kết nối để lại kết nối ma giữ người đó online vô hạn. Typing: giới hạn tần suất dùng bộ nhớ nên vẫn chạy |
| Push API không gọi được | Bỏ cập nhật trạng thái sau 3 lần thử; client lấy lại khi `/v1/state` hoặc watch lại |
| Sự kiện lifecycle mất (gateway crash không phát) | Router phát `NODE_DOWN`; thiếu cả hai thì TTL 24 h của `pres:*` |
| Rebalance `chat-receipts` | Lệnh xử lý lại sau khi commit cuối: ghi lại mốc cũ, `USING TIMESTAMP` làm nó vô hại; có thể đẩy trùng |

**Metrics:** `chat.receipts.lag` (record, giây), `chat.receipts.rejected{reason}`, `chat.receipts.pushes`, `chat.typing.relayed`, `chat.typing.dropped{reason}`, `chat.presence.transitions{to}`, `chat.presence.watchers` (gauge ước lượng), `chat.presence.pushes`, `chat.presence.unavailable`, `chat.state.requests{route,code}`.

**Cấu hình** (`chat.yml`): cửa sổ gộp (DM 500 ms, nhóm 2 s), mốc nhóm có danh sách (50), ân hạn offline (5 s), TTL watch (5 phút), tối đa mục watch (200), giới hạn mốc/typing. Push API: `TenantCfg` của `chat` cần `push_rate` đủ cho thêm EPHEMERAL (ước lượng ở bước đo đạc, §10).

---

## 10. Kiểm thử

| Tầng | Nội dung |
|---|---|
| Unit | Kiểm tra lệnh §3.3 theo case testkit; gộp mốc theo cửa sổ (lấy max, thứ tự đảo); chọn người nhận theo loại và cỡ hội thoại; lọc quyền §8 (mọi ô của bảng); ân hạn 5 s (nối lại trong 4 s không phát gì, 6 s thì phát offline); làm tròn và "gần đây" của lần cuối hoạt động; giới hạn typing |
| IT (Testcontainers ScyllaDB, Kafka, Redis) | Mốc tới sai thứ tự / trùng không làm lùi (bất biến 1); hai ReceiptWorker giành partition không làm lùi mốc; `NODE_DOWN` xoá kết nối của node đó; watch hết hạn tự rơi; `/v1/state?since` trả đúng hội thoại có thay đổi; migration `V3` chạy lại không lỗi |
| E2E IT (chat-service, Push API, router, 2 gateway, client devtools) | DM: Đã nhận rồi Đã xem tới người gửi; nhóm 3: danh sách đã xem; nhóm 60: `readCount`; đọc trên thiết bị 1 → `own_read_seq` tới thiết bị 2; typing tới người kia và không tới người ngoài hội thoại; B ngắt → A (đang watch) nhận offline sau ~5 s; B ngắt rồi nối lại trong 2 s → A không nhận gì; người không chung hội thoại watch B → không nhận gì; `hideRead` và `hidePresence` có qua có lại; hợp đồng §7: B mất 3 tin giữa chừng, thấy lỗ, `/v1/sync` bù đúng, không trùng |
| Devenv | `make smoke` thêm `CHAT receipts`, `CHAT typing`, `CHAT presence` |
| Đo đạc | Nhóm 50 người cùng đọc; 10 000 user online mỗi người watch 50 người, 1% chập chờn mỗi phút: số push presence mỗi giây và độ trễ |

---

## 11. Các cách đã cân nhắc

| Cách | Được | Mất | Kết luận |
|---|---|---|---|
| **A. Trong chat-service; client đăng ký người cần xem online** | Tốn ít; một service; luật "chung hội thoại" kiểm tại chỗ | Client phải gửi lại danh sách khi đổi màn hình | **Chọn** (S7) |
| B. Presence là service riêng của nền tảng (scope `presence:read`) | Dùng lại cho app khác | Thêm service để vận hành; vẫn phải hỏi chat-service luật chung hội thoại; chưa có app thứ hai | Để sau; A tách ra được thành B |
| C. Trong chat-service; báo online cho mọi người chung hội thoại mỗi khi đổi | Client đơn giản nhất | 10 nhóm × 500 người = ~5 000 push mỗi lần một người chập chờn | Loại |
| Dùng `DeliveredRecord` của router cho "đã nhận" | Có sẵn, chính xác theo thiết bị | Một bản ghi Kafka mỗi tin mỗi thiết bị; app cần theo người | Loại |

---

## 12. Để cho người hiện thực quyết

- Cấu trúc hàng đợi trễ cho ân hạn 5 s (bộ nhớ, Redis sorted set, …) miễn đúng hành vi §6.1.
- Cách cache `conversations.last_seq` cho việc kẹp mốc (§3.3).
- Chia `user_convs.state_at` cho kênh hay không (kênh hiện dựa vào `conversations.last_seq`).
- Số thử lại và backoff cụ thể của đẩy trạng thái, trong giới hạn "bỏ sau 3 lần".

## 13. ADR dự kiến

- 0037 Mốc đã nhận / đã xem theo người, chỉ tăng bằng `USING TIMESTAMP`, không dùng receipt của router.
- 0038 Online theo đăng ký của client (watch), luật chung hội thoại, ân hạn 5 s; Redis sập thì online tạm sai.
- 0039 Hợp đồng đồng bộ phía client (`syncedSeq`, lỗ → `/v1/sync`, `/v1/state` sau khi nối lại).
