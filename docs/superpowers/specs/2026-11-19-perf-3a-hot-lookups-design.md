# Đợt 3a: tra cứu nóng (thành viên, giới hạn tần suất, badge)

- **Trạng thái:** Đã duyệt (2026-11-19). Đã đọc lại một lượt (§7 ghi những gì đã sửa).
- **Nguồn:** review toàn bộ chat 2026-11-19 (CH1 I5/I6/I8, CH3 I3/I5/I7, CH4 I2/I6, cross I2/I6). Đợt 3 có ba phần: **3a** (tài liệu này), 3b client Push API, 3c vòng Kafka. Thứ tự và cách chia do chủ dự án duyệt ngày 2026-11-19.

## 1. Mục tiêu và vì sao

Chủ dự án chọn thiết kế cho **quy mô mục tiêu: 1 triệu người dùng, kênh 100 000 thành viên**, chấp nhận phần lớn không đo trực tiếp được trên máy dev 4 vCPU.

Ở quy mô đó, chỗ gãy đầu tiên là kênh lớn: mỗi lệnh của một người theo dõi (gửi, đánh dấu đã nhận/đã xem, gõ phím, theo dõi online) tải **cả danh sách thành viên** (một chuỗi Redis ~3,5 MB cho 100k người, hoặc đọc cả partition ScyllaDB khi cache trống) chỉ để biết một người có trong đó không. 100k người đánh dấu đã xem một bài ≈ 350 GB lưu lượng Redis. Giới hạn tần suất lại đặt *sau* bước tra, nên người lạ gửi lệnh tới hội thoại bất kỳ cũng tốn những lần đọc đó. Thứ hai là badge: mỗi thông báo offline đọc và cộng **toàn bộ** inbox (`user_convs`) của người nhận.

**Thành công:** mọi lệnh và mọi API của một người trong một hội thoại tốn **số lần đọc store không phụ thuộc số thành viên** của hội thoại (trừ hai việc hiếm, ghi ở §3.5) (kiểm bằng test đếm lời gọi store với kênh 100k); badge của một thông báo tốn **O(1)** lần gọi Redis; mỗi API đọc có giới hạn tần suất theo người dùng; mọi giới hạn kiểm *trước* mọi lần tra store.

**Không làm (non-goals):** song song hoá vòng Kafka, ghi inbox theo lô (3c); pool client Push API (3b); đánh chỉ mục lại danh sách hội thoại theo thời gian; badge tính cả kênh (giữ quy tắc CH4 §3.1 hiện tại).

**Phản mục tiêu:** một thành viên vừa bị xoá vẫn gửi/nhận được vì đọc từ cache cũ (bất biến hiện có: "người bị xoá không nhận gì sau thay đổi"); badge lệch dần theo thời gian mà không tự sửa; Redis sập làm hỏng gửi tin.

## 2. Cách tiếp cận đã cân nhắc

| Cách | Được | Mất |
|---|---|---|
| **A. Tra từng người từ ScyllaDB** (`members` theo khoá `(conv_id, user_id)`, không cache) — **chọn** | Luôn mới (giữ bất biến xoá thành viên), O(1), không thêm cấu trúc dữ liệu | Thêm một lần đọc ScyllaDB (~1 ms) cho mỗi lệnh; chịu được vì có giới hạn tần suất trước |
| B. Redis HASH `mem:{conv}` cập nhật từng người khi vào/ra | Nhanh hơn A | Phải làm đầy cả 100k người khi cache trống, và giữ đồng bộ với mọi thay đổi (dấu evict, đua); thêm một nguồn sự thật |
| C. Giữ chuỗi danh sách, thêm cache trong bộ nhớ mỗi pod | Ít sửa | Vẫn tải 3,5 MB mỗi lần hụt; cache mỗi pod làm người bị xoá còn quyền tới khi hết hạn (vi phạm phản mục tiêu) |

Badge:

| Cách | Được | Mất |
|---|---|---|
| **X. Bộ đếm chưa đọc duy trì trong Redis** (hash theo người dùng, cập nhật idempotent theo kiểu "lấy max") — **chọn** | O(1) mỗi thông báo; idempotent khi Kafka giao lại | Thêm một lệnh Redis cho mỗi thành viên mỗi tin; phải tự dựng lại khi mất |
| Y. Giữ quét, chỉ giảm giới hạn 10 000 | Không sửa gì | Vẫn O(số hội thoại) mỗi thông báo |
| Z. Bộ đếm cộng/trừ đơn giản (`INCR`/`DECR`) | Rẻ nhất | Giao lại tin hay đánh dấu trùng làm lệch vĩnh viễn |

## 3. Thiết kế

### 3.1 Tra thành viên từng người (H1)

- `MemberLookup.member(convId, userId)` → `Optional<Role>`: đọc **trực tiếp** `ChatStores.MemberStore.member()` (một dòng ScyllaDB), không qua cache. Đây là đường duy nhất để kiểm "có phải thành viên".
- `MemberLookup.of(convId)` (cả danh sách, cache Redis 60 s như cũ) chỉ còn dùng cho **DM và nhóm** (≤ 500 người): fan-out, danh sách người nhận biên nhận, `peer` của DM, `/receipts`. **Không bao giờ** gọi cho kênh.
- Đổi từng chỗ gọi:

| Chỗ | Trước | Sau |
|---|---|---|
| `Ingest.check` | cả danh sách | `member(sender)`; DM: thêm `member(người kia)` + `users.isDeleted` cho đúng một người kia (DM luôn 2 người: lấy người kia từ `of(convId)` của DM là hợp lệ, ≤ 2 dòng) |
| `MarkValidator.validate` | cả danh sách | `member(user)` |
| `TypingRelay.typing` | cả danh sách, rồi mới kiểm kênh | kiểm loại hội thoại trước (kênh → `channel`); rồi `member(user)`; danh sách (≤ nhóm) chỉ tải khi đã qua giới hạn |
| `PresenceWatch` | đã từ chối kênh trước khi tải danh sách; nhưng mỗi mục watch gọi `conversations.get` một lần và tải danh sách (≤ 500, cache) hai lần | giữ danh sách có cache (chỉ DM / nhóm); **mỗi hội thoại chỉ tra một lần cho cả lệnh watch** (một watch 200 mục hay trỏ nhiều mục vào cùng hội thoại) |
| `PresenceFanOut.on` | danh sách có cache, chỉ DM / nhóm (watch không bao giờ trỏ vào kênh) | không đổi |
| `ReceiptWorkerLoop.flushDue` | cả danh sách mọi loại | DM / nhóm: như cũ; **kênh: không tra lại**. Thành viên đã được kiểm ở `MarkValidator` lúc nhận lệnh; với kênh, worker chỉ ghi mốc của chính người đó và đẩy `own_read_seq` cho chính họ, nên một người vừa rời trong khoảng 2 s đó không lộ gì cho ai (`conv_marks` không bao giờ bị xoá, dòng `user_convs` đã đánh dấu `left`) |

### 3.2 Giới hạn tần suất trước khi tra (H2) và cho API đọc (H3)

- `Ingest`, `MarkValidator`, `TypingRelay`: thứ tự mới là **kiểm định dạng → giới hạn theo người dùng → mới tra store**. Giới hạn theo hội thoại (`perConv` của Ingest) giữ sau bước tra thành viên (không cho người lạ tiêu hạn mức của một hội thoại).
- API đọc chưa có giới hạn nhận `TokenBucketLimiter` theo người dùng, mỗi pod (như các giới hạn hiện có), quá thì `429 rate_limited`:

| API | Tốc độ / burst (đề xuất của người viết) |
|---|---|
| `GET /v1/conversations` | 5/s, burst 20 |
| `GET /v1/state` | 2/s, burst 10 |
| `POST /v1/sync` | 2/s, burst 10 |
| `GET /v1/presence` | 2/s, burst 10 |
| `GET /v1/conversations/{id}/messages` (lịch sử) | 20/s, burst 50 (đủ cho client lật trang hay bắt kịp sau khi nối lại) |
| Quản lý: thêm / xoá thành viên (kể cả tự rời), đổi vai trò, chuyển chủ, tự vào kênh, tạo nhóm / kênh, mở DM | **chung một** bucket mỗi người: 5/s, burst 20 |
| Còn lại: `GET /v1/conversations/{id}`, `PUT .../mute`, `GET` / `PUT /v1/me/privacy` | chung một bucket mỗi người: 10/s, burst 30 |

  Cấu hình trong `chat.limits`, mặc định như bảng (quyết định của người viết).

### 3.3 Badge từ bộ đếm duy trì (H4)

- Redis hash `badge:{user}`: mỗi hội thoại (DM, nhóm; **không** kênh) một field `<convId>` = `"<last>:<read>"`, và field `_total`. TTL 30 ngày, gia hạn mỗi lần ghi.
- Một script Lua `raise(user, conv, last, read)`: **không làm gì nếu `badge:{user}` chưa tồn tại** (chưa dựng hoặc đã mất: lần đọc sau dựng lại từ ScyllaDB, nơi thay đổi này đã được ghi trước); nếu có thì đọc field cũ, lấy `max` từng phần (0 = không đổi), tính lại `unread = max(0, last - read)`, cộng chênh lệch vào `_total`. Idempotent: gửi lại cùng giá trị không đổi gì; giá trị nhỏ hơn không lùi. Thêm `drop(user, conv)` (rời hội thoại: trừ phần chưa đọc, xoá field; cũng không làm gì khi hash không tồn tại).
- **Thứ tự:** mọi `raise` / `drop` chạy **sau** khi ghi ScyllaDB tương ứng xong, nên ScyllaDB luôn đi trước bộ đếm và lần dựng lại luôn thấy thay đổi.
- Ai gọi:
  - fan-out, khi nâng `user_convs.last_seq` của một thành viên (`touchAll`): `raise(member, conv, seq, 0)`; người gửi (`senderMarks`): `raise(sender, conv, seq, seq)`. Nhóm 500 người là 500 lệnh Redis mỗi tin: gửi **tất cả cùng lúc rồi mới chờ trả lời** (Lettuce pipeline, như `onlineOf`), không tuần tự, để 3a không làm fan-out nhóm chậm thêm trước khi 3c ghi inbox theo lô;
  - worker biên nhận, khi nâng `read_seq`: `raise(user, conv, 0, read)`;
  - thêm vào nhóm (F1): `raise(user, conv, last, last)`; rời / bị xoá: `drop`.
- Đọc (`Badge.total(user, conv, seq)`): `raise(user, conv, seq, 0)` rồi đọc `_total` trong **cùng** một script (tin đang báo luôn được tính, như F4).
- **Khi chưa có hoặc đã mất** (`badge:{user}` không tồn tại: người dùng cũ, Redis mất dữ liệu, hết TTL): đọc `user_convs` một lần như hiện nay (giới hạn 10 000; bỏ dòng `left` và dòng kênh), ghi cả hash, rồi trả. Chỉ người nhận thông báo mới tốn lần dựng lại này, nên hash chỉ tồn tại cho người từng nhận thông báo offline trong 30 ngày.
- **Khi ghi lỗi:** một lần `raise`/`drop` thất bại (Redis lỗi) thì `DEL badge:{user}` (best effort) để lần đọc sau dựng lại từ ScyllaDB; không bao giờ để bộ đếm lệch âm thầm. Lỗi Redis không làm hỏng fan-out hay biên nhận (badge là phụ).
- **Tự sửa định kỳ** (quyết định của người viết, sau builder check): một lần ghi bị mất mà Redis vẫn còn hash (Redis không gọi được nên cả `DEL` cũng hỏng, failover mất vài giây ghi, pod cũ chưa gọi `raise` trong lúc deploy cuốn chiếu) chỉ tự sửa ở lần có tin hay mốc mới của chính hội thoại đó; một `drop` mất thì không bao giờ. Vì thế hash có field `_built_at`: lần đọc thấy hash dựng quá **24 giờ** thì dựng lại từ ScyllaDB. Badge lệch tối đa một ngày, không vĩnh viễn.
- **Dựng lại là một script gộp** (`rebuild(user, snapshot)`), không ghi đè: với mỗi hội thoại trong ảnh chụp từ ScyllaDB, field mới = `max` từng phần của field đang có và ảnh chụp (một `raise` chạy cùng lúc không bị kéo lùi; một lần ghi bị mất được ảnh chụp bù lên); field có trong hash mà **không** có trong ảnh chụp (một `drop` bị mất) bị xoá; rồi tính lại `_total` từ đầu và đặt `_built_at`. Một người vừa vào nhóm đúng lúc ảnh chụp được đọc có thể mất field của nhóm đó tới lần có tin kế tiếp hoặc lần dựng lại sau (lệch nhỏ, tự sửa).
- **Bộ nhớ Redis:** khoảng 30 byte mỗi hội thoại; một người nhận thông báo với 500 hội thoại ≈ 15 KB; 1 triệu người như vậy ≈ 15 GB. Giới hạn bởi TTL 30 ngày và việc chỉ người nhận thông báo mới có hash.

### 3.4 Danh sách hội thoại và `/v1/state` (H5)

- Giữ đọc cả partition `user_convs` (giới hạn 10 000) — chấp nhận ở quy mô mục tiêu (người dùng điển hình < 500 hội thoại; 1 000 dòng ≈ 1 ms) nhờ giới hạn tần suất ở H3.
- `lastSeq` của kênh trong mỗi mục đọc qua `LastSeqCache` (5 s) thay vì đọc ScyllaDB mỗi mục.
- Ghi vào giới hạn đã biết của README: một người trong > 10 000 hội thoại thấy danh sách và badge bị cắt.

### 3.5 Ngoại lệ đã biết (quyết định của người viết)

- **Chuyển quyền chủ khi chủ rời hoặc xoá tài khoản** (`handOverOwnership`) và nhánh tương thích `ownerOf` cho hội thoại tạo trước V8 vẫn đọc cả partition `members` (tìm admin / thành viên vào sớm nhất). Đây là việc hiếm (một lần mỗi lần chủ rời), nên giữ; tìm "người vào sớm nhất" mà không quét cần một bảng chỉ mục mới, không đáng ở 3a. Test đếm lời gọi ở §4 ghi rõ hai ngoại lệ này.

## 4. Kiểm chứng

- **Đếm lời gọi:** test đơn vị với `FakeStores` có kênh 100 000 thành viên: một lệnh gửi / đánh dấu / gõ phím và một lần flush biên nhận của kênh gọi `members()` (và cache danh sách) **0 lần**, `member()` ≤ 2 lần. Một lệnh watch 200 mục trỏ vào 10 hội thoại gọi `conversations.get` ≤ 10 lần.
- **Giới hạn trước tra:** người không phải thành viên gửi dồn lệnh tới hội thoại ngẫu nhiên → sau burst nhận `rate_limited` và không còn lần tra store nào.
- **Badge:** test thuộc tính: chuỗi thao tác ngẫu nhiên (tin mới, đánh dấu, giao lại, vào, rời, mất hash, `raise` khi hash chưa có) → `_total` luôn bằng tổng tính lại từ `user_convs`; `raise` khi hash chưa có không tạo hash. Thêm: một lần ghi bị mất (hash vẫn còn) → lệch tới khi `_built_at` quá 24 giờ, rồi đúng lại. IT Redis thật cho script Lua.
- **Xoá thành viên:** IT có sẵn `removedMemberStopsReceivingAtOnce` vẫn xanh; thêm: thành viên bị xoá đánh dấu đã xem → `not_member` ngay.
- Không có số đo tải mới trong 3a (đo ở 3c).

## 5. Giới hạn theo pod

Mọi giới hạn ở §3.2 là theo pod, như các giới hạn hiện có: với N pod sau load balancer, một người đi qua nhiều pod có thể được tới N lần mức trong bảng. Chấp nhận ở 3a (vẫn chặn được việc tốn đọc không giới hạn); giới hạn dùng chung qua Redis để sau nếu cần.

## 6. Để người xây dựng tự quyết

Tên lớp và phương thức; vị trí script Lua (resource); cách tiêm các limiter; dùng lại hay tách `RedisChatCache` cho badge.

## 7. Đọc lại lần một (2026-11-19)

Đối chiếu với code, đã sửa:
- **Lỗi thiết kế:** `raise` trên hash chưa tồn tại sẽ tạo một hash chỉ có một hội thoại; quy tắc "hash không tồn tại thì dựng lại" sẽ không bao giờ chạy và badge sai mãi. Nay `raise` / `drop` không làm gì khi hash chưa có.
- **Lỗi thiết kế:** dựng lại bằng `HSETNX` không sửa được field đã có (chính là trường hợp lần dựng lại 24 giờ cần sửa) và không xoá field của `drop` bị mất. Nay dựng lại gộp bằng `max` và xoá field thừa.
- Fan-out nhóm 500 người sẽ chậm thêm 500 lệnh Redis tuần tự mỗi tin: nay gửi pipeline.
- `PresenceWatch` / `PresenceFanOut` đã không bao giờ tải danh sách kênh (kiểm loại trước); đổi sang đọc ScyllaDB từng người sẽ *tốn hơn* danh sách có cache. Giữ danh sách, chỉ bỏ tra trùng trong một lệnh watch.
- Worker biên nhận với kênh: tra từng người trong mỗi lần flush (hàng nghìn lần đọc mỗi 2 s, một luồng) tệ hơn bỏ hẳn việc tra lại; thành viên đã được kiểm lúc nhận lệnh.
- Thêm ước lượng bộ nhớ Redis của badge và ghi rõ giới hạn tần suất là theo pod.
