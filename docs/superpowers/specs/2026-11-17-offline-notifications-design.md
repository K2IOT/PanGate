# Thông báo đẩy khi offline (CH4): notify-service và thông báo của app chat — thiết kế

- **Trạng thái:** bản nháp, chờ chủ dự án duyệt
- **Ngày:** 2026-11-17
- **Vị trí trong lộ trình app chat:** sub-project 4/4, cuối cùng. (CH1) Chat core, (CH2) Tài khoản, (CH3) Trạng thái hội thoại đã xong.
- **Dựa trên:** spec chat core `2026-11-12-chat-core-design.md` ("CH1 §n"), spec tài khoản `2026-11-16-account-service-design.md` ("CH2 §n"), spec trạng thái hội thoại `2026-11-17-conversation-state-design.md` ("CH3 §n"), contracts push `2026-11-10-push-contracts-design.md` ("contracts §n"), ADR 0031 (fan-out của chat), ADR 0036 (thu hồi theo thiết bị).

---

## 0. Mục tiêu, phạm vi

**Mục tiêu.** Người dùng không mở app trên thiết bị nào vẫn biết có tin mới: điện thoại iOS (APNs) và Android (FCM) nhận thông báo đẩy có tên người gửi, trích đoạn tin, và số chưa đọc trên icon app (badge). Đây là phần các spec trước đã hẹn cho CH4 (CH1 D4, CH2 §2, CH3 §0, ADR 0031).

**Quyết định đã chốt khi brainstorm (2026-11-17):**

| # | Câu hỏi | Quyết định |
|---|---|---|
| N1 | Đang mở app trên máy tính thì điện thoại có nhận thông báo không | **Không**, như Telegram: chỉ đẩy khi không thiết bị nào nhận được tin. Đúng hợp đồng router hiện có: `OfflineRecord` chỉ sinh khi không kết nối nào giao được (contracts §1.1a). Không sửa router |
| N2 | Thông báo hiện gì | Tên người gửi, tên nhóm/kênh, trích đoạn tin. Mỗi người có cài đặt **ẩn xem trước**; bật thì chỉ hiện "Tin nhắn mới". Nội dung đi qua máy chủ Apple/Google, chấp nhận được vì app không mã hoá đầu-cuối (CH1 D7) |
| N3 | Kênh có đẩy không | **Có**, như Telegram: một worker duyệt người theo dõi theo trang, gửi có giới hạn tốc độ, bài sau thay thông báo của bài trước |
| N4 | Phạm vi đã duyệt | Đăng ký push token theo thiết bị; gỡ token khi app đổi token, khi đăng xuất / khoá / đổi mật khẩu / xoá tài khoản, khi Apple/Google báo token chết; tắt thông báo theo hội thoại (1 giờ, 8 giờ, vĩnh viễn; vẫn tính vào badge); gộp thông báo theo hội thoại; kiểm bằng APNs/FCM giả lập |
| N5 | Cách dựng | **Tách đôi.** Service nền tảng mới `notify-service` giữ token, khoá Apple/Google và gửi đi; chat-service quyết định có đẩy không và nói gì |

**Tiêu chí thành công.**
- `make smoke` có bước `CHAT notify`: b đăng ký token APNs giả cho thiết bị của mình rồi ngắt kết nối; a gửi DM; APNs giả nhận đúng một thông báo, tiêu đề là tên a, nội dung là tin, badge đúng số chưa đọc của b. Sau khi b đăng xuất thiết bị đó, tin tiếp theo không sinh thông báo.
- b đang online ở bất kỳ thiết bị nào thì không có thông báo (N1).
- Hội thoại đã tắt thông báo không sinh thông báo, trừ khi b được nhắc tên (§3.3). Bật "ẩn xem trước" thì thông báo không chứa tên hay nội dung.
- Kênh 100 000 người theo dõi offline: mọi người theo dõi chưa tắt kênh nhận đúng một thông báo cho một bài; hai bài liên tiếp không đẩy đủ hai lượt (§4.2). Đo thời gian hết lượt trên máy dev.
- Apple/Google trả "token chết" thì token bị xoá, lần sau không gửi tới nó nữa.

**Ngoài phạm vi CH4:**
- trả lời ngay từ thông báo, nút hành động trong thông báo;
- giờ yên lặng, lịch tắt thông báo;
- email, SMS, web push;
- thông báo có ảnh, rich notification;
- **gỡ thông báo hay cập nhật badge khi người dùng đọc ở thiết bị khác** (silent push "đã đọc"): badge trên điện thoại được cập nhật ở thông báo kế tiếp hoặc khi app mở và đồng bộ (CH3). Quyết định của người viết, xem §9;
- app iOS/Android thật; khoá Apple/Google thật (cấu hình khi có app);
- nhiều cell.

**Bất biến:**
1. Không gửi thông báo cho một người đang có ít nhất một thiết bị nhận được tin đó (N1). Với kênh: không gửi cho người đang online (§4.2).
2. Token của một thiết bị đã bị thu hồi (đăng xuất, khoá, đổi mật khẩu, xoá tài khoản) không nhận thông báo nào sau khi `notify-service` đọc được lệnh thu hồi trên `gw.control`.
3. Một push token chỉ thuộc một thiết bị: đăng ký lại cùng token cho thiết bị khác (cài lại app, đổi tài khoản) gỡ nó khỏi thiết bị cũ. Một chiếc điện thoại không bao giờ nhận thông báo của tài khoản đã đăng xuất trên nó.
4. Một tin sinh tối đa một thông báo trên mỗi thiết bị, kể cả khi `OfflineRecord` hay yêu cầu thông báo bị đọc lại (§5.4).

---

## 1. Kiến trúc

```
                          ┌──────────── chat-service ────────────┐
router ─▶ c1.push.offline ─▶ OfflineNotifier ─┐                  │
                          │                   ├─▶ c1.notify.requests ─▶ notify-service ─▶ APNs / FCM
chat fan-out (kênh) ─▶ chat.channel.notify ─▶ ChannelNotifier ─┘     │   ▲         │
                          └──────────────────────────────────────┘     │   │         └─ PostgreSQL (schema notify)
                                                                       │   └─ gw.control (thu hồi)
app ── PUT /v1/devices/self/push (connect token) ──────────────────────┘
```

- **notify-service** (mới, của nền tảng, không biết gì về chat): API đăng ký token cho thiết bị đang đăng nhập; đọc `c1.notify.requests` và gửi tới mọi token của principal (hoặc của một thiết bị); xoá token chết; đọc `gw.control` để xoá token của thiết bị/principal bị thu hồi. Dùng được cho mọi tenant: cấu hình khoá Apple/Google theo tenant.
- **chat-service** (thêm): `OfflineNotifier` đọc `c1.push.offline` của tenant `chat`; `ChannelNotifier` đẩy bài kênh; cả hai soạn thông báo và produce `NotifyRequest`. Thêm API tắt thông báo theo hội thoại và cài đặt ẩn xem trước.
- **account-service** (sửa nhỏ): sự kiện `account.users` mang thêm tên hiển thị, để chat-service ghi tên người gửi vào thông báo (§3.2).
- **Router, gateway, Push API:** không đổi.

**Phụ thuộc lúc chạy của notify-service:** PostgreSQL (schema `notify`), Redis (dedup, §5.4), Kafka (`c1.notify.requests`, `gw.control`), auth-service (JWKS để kiểm connect token), APNs và FCM (hoặc bản giả lập ở devenv).

---

## 2. Hợp đồng (platform-contracts)

### 2.1 `NotifyRequest` trên `c1.notify.requests`

File mới `pangate/v1/notify.proto`:

```proto
// cN.notify.requests — producers: tenant backends (chat-service); consumer: notify-service (group "notify").
message NotifyRequest {
  string request_id = 1;             // dedup key, unique per (event, recipient); e.g. "<msgId>/<principal>"
  string tenant_id = 2;
  oneof target {
    string principal_id = 3;         // every registered device of the principal
    DeviceRef device = 4;            // one device
  }
  Alert alert = 5;
  int32 badge = 6;                   // >= 0 sets the badge; -1 leaves it unchanged
  string thread_id = 7;              // groups notifications on the device (APNs thread-id, FCM tag)
  string collapse_id = 8;            // a later notification with the same id replaces the earlier one
  map<string, string> data = 9;      // passed to the app as is (e.g. conversationId, convSeq, msgId)
  int64 created_at_ms = 10;
  int64 expires_at_ms = 11;          // not sent after this; also the APNs apns-expiration / FCM ttl
  bool high_priority = 12;           // APNs priority 10 / FCM HIGH; false: 5 / NORMAL
}

message Alert {
  string title = 1;                  // shown as is when set
  string body = 2;
  string loc_key = 3;                // instead of title/body: the app localizes (APNs loc-key, FCM body_loc_key)
  repeated string loc_args = 4;
}
```

- Key Kafka: `principal_id` (hoặc `principal_id` của `device`), để các yêu cầu của một người đi theo thứ tự. Header theo quy ước `PushHeaders` (contracts §2): `tenant`, `content-type: application/x-protobuf; schema=pangate.v1.NotifyRequest`.
- Topic `c1.notify.requests`: 32 partition, giữ 1 ngày (yêu cầu cũ hơn đã hết hạn). Testkit thêm `NotifyTopics`, `NotifyKeys`, `NotifyRules` (kiểm hợp lệ: có `request_id`, target, `expires_at_ms > created_at_ms`, alert có title/body hoặc loc_key, thân `Alert` ≤ 2 KB) và `NotifyCases`.

### 2.2 `AccountState.display_name`

Thêm `string display_name = 4;` vào `AccountState` (`account.proto`). account-service phát `account.users` cả khi đổi tên hiển thị (hiện chỉ phát khi đổi trạng thái), qua outbox như cũ (CH2 §6).

---

## 3. chat-service: thông báo cho DM và nhóm

### 3.1 Đọc `c1.push.offline`

`OfflineNotifier`, consumer group `chat-notify`, chỉ xử lý `OfflineRecord` có `record.tenant_id = "chat"` và header `type = chat.message` trong frame; còn lại bỏ qua. Với mỗi bản ghi:

1. Giải `ChatMessage` từ `record.frame.payload`; người nhận là `record.principal_id` (`chat:<userId>`).
2. **Quá cũ thì bỏ:** tin gửi cách đây hơn 1 giờ (`sentAt`) thì không đẩy, có metric. Tránh việc notify hoặc chat-service ngừng vài giờ rồi xả một loạt thông báo cũ.
3. **Kiểm tắt thông báo** (§3.3) và **ẩn xem trước** (§3.4).
4. **Badge:** tổng chưa đọc của người nhận trên mọi hội thoại chưa rời, tính như CH3 (`last_seq - read_seq`, không âm), gồm cả hội thoại đã tắt (N4). Đọc một partition `user_convs` của người nhận.
5. Soạn và produce `NotifyRequest` (§3.5), `request_id = "<msgId>/<userId>"`. Commit offset sau khi Kafka ack (không mất, có thể trùng; notify dedup, §5.4).

Store lỗi tạm thì thử lại như các loop khác của chat-service (lùi dần, không chết thread). Bản ghi không giải được: bỏ, có metric.

### 3.2 Tên người gửi và tên hội thoại

- Bảng `users` của chat-service thêm cột `display_name`, ghi từ `account.users` (§2.2) bởi relay đã có của CH2 (bỏ qua trạng thái cũ hơn theo `updated_at_ms`). Thiếu tên thì dùng `userId`.
- Tên hội thoại: `conversations.title` cho nhóm và kênh. DM không có tên: tiêu đề là tên người gửi.
- Cache tên trong bộ nhớ theo pod (60 s), vì nhóm sôi nổi có nhiều thông báo cho cùng người gửi.

### 3.3 Tắt thông báo theo hội thoại

- `PUT /v1/conversations/{id}/mute` thân `{"until": <epoch ms> | null}`: `null` là vĩnh viễn, thời điểm đã qua là bật lại. Client đưa 1 giờ / 8 giờ dưới dạng thời điểm. Chỉ thành viên mới gọi được (403 nếu không).
- Lưu `muted_until bigint` (0 = không tắt, `Long.MAX_VALUE` = vĩnh viễn) trên **`members`** (để worker kênh đọc cùng lúc khi duyệt người theo dõi, §4) và trên `user_convs` (để danh sách hội thoại trả `mutedUntil`). Cột `muted boolean` cũ của `user_convs` không dùng, để nguyên.
- Đang tắt thì không đẩy, **trừ khi người nhận có trong `mentions` của tin** (như Telegram; quyết định của người viết, §9). Vẫn tính vào badge.

### 3.4 Ẩn xem trước

`user_settings` thêm cột `hide_preview boolean`. `GET/PUT /v1/me/privacy` thêm trường `hidePreview` (ghi từng trường như CH3, review PR #6).

### 3.5 Nội dung thông báo

| Loại | `title` | `body` |
|---|---|---|
| DM | tên người gửi | nội dung tin |
| Nhóm | tên nhóm | `<tên người gửi>: <nội dung>` |
| Kênh (§4) | tên kênh | nội dung |
| Ẩn xem trước | (trống) | (trống); `loc_key = "notif_new_message"`, app tự hiện "Tin nhắn mới" theo ngôn ngữ máy |

- Nội dung cắt ở 200 ký tự (theo code point, thêm "…"). Tin chỉ có `replyTo`/không có chữ thì dùng `loc_key = "notif_new_message"`.
- `thread_id = conversationId` (gộp theo hội thoại trên máy, N4); `collapse_id = msgId` (đọc lại không ra hai dòng); `data = {conversationId, convSeq, msgId}`; `high_priority = true`; `expires_at_ms = sentAt + 1 giờ`.

---

## 4. chat-service: thông báo cho kênh

### 4.1 Kích hoạt

Khi fan-out một bài kênh (BROADCAST, CH1 §6), chat-service produce một job `ChannelNotifyJob{convId, convSeq, msgId, sentAt}` vào topic nội bộ `chat.channel.notify` (key `convId`, 16 partition). Job chỉ cần bài đã lưu bền; không chặn fan-out.

### 4.2 Worker `ChannelNotifier`

- Consumer group `chat-channel-notify`. Một job duyệt `members` của kênh theo trang 1 000 dòng (partition `conv_id`, thứ tự `user_id`).
- Với mỗi trang: bỏ người đã tắt kênh (`muted_until`, trừ khi được nhắc tên), bỏ người gửi, bỏ người **đang online** (presence của CH3: `pres:{user}` khác rỗng, đọc pipeline cả trang), bỏ người bật ẩn xem trước khỏi phần nội dung (vẫn đẩy, dạng `loc_key`). Produce một `NotifyRequest` mỗi người còn lại: `badge = -1` (không đổi badge, xem §9), `thread_id = convId`, **`collapse_id = convId`** (bài sau thay thông báo của bài trước trên máy).
- **Bài mới thay job cũ:** chỉ giữ một job sống mỗi kênh. Job mới tới khi job cũ chưa xong thì job cũ dừng sau trang hiện tại, job mới duyệt lại từ đầu. Người chưa nhận bài cũ sẽ nhận bài mới (cùng `collapse_id`), không ai nhận hai lượt cho hai bài liên tiếp. Bài cũ hơn 1 giờ thì job bỏ.
- **Giới hạn tốc độ:** mỗi pod tối đa 5 000 `NotifyRequest`/s cho kênh (`chat.notify.channel-rate`), dùng chung cho mọi kênh; DM/nhóm không bị giới hạn này.
- **Bền qua restart:** sau mỗi trang ghi điểm dừng `(convId, convSeq, lastUserId)` vào bảng `channel_notify_progress` (Scylla, TTL 1 ngày) rồi mới commit offset khi job xong. Pod chết giữa chừng: pod nhận partition đọc lại job và đi tiếp từ điểm dừng. Trang cuối có thể bị gửi lại; notify dedup theo `request_id = "<msgId>/<userId>"`.

---

## 5. notify-service

### 5.1 Đăng ký token

API công khai (qua cùng ingress với chat-service), xác thực bằng connect token như chat-service: chữ ký và `aud=pangate-gw` qua JWKS của auth-service, `exp`, và chưa bị thu hồi theo jti / device / principal (đọc `gw.control`, giữ danh sách thu hồi trong bộ nhớ như chat-service). `tenant`, `principal`, `device` lấy từ token; client không tự chọn thiết bị nào.

| API | Thân | Kết quả |
|---|---|---|
| `PUT /v1/devices/self/push` | `{"platform": "apns" \| "fcm", "token": "...", "environment": "production" \| "sandbox"}` (`environment` chỉ cho APNs, mặc định `production`) | `204`. Ghi đè token cũ của thiết bị; gỡ cùng token khỏi mọi thiết bị khác (bất biến 3) |
| `DELETE /v1/devices/self/push` | — | `204` (cả khi chưa có) |

- Token: APNs là chuỗi hex 64–200 ký tự; FCM tối đa 4 096 ký tự. Sai định dạng → `400 invalid_request`. Giới hạn 10 lần/phút mỗi thiết bị → `429`.
- App gọi `PUT` mỗi lần mở và mỗi khi hệ điều hành cấp token mới; `updated_at` được làm mới. Token không làm mới 180 ngày (bằng `account.session-idle-ttl`, CH2 §2) thì job dọn hằng ngày xoá.

### 5.2 Lưu trữ (PostgreSQL, schema `notify`)

```sql
CREATE TABLE notify.push_tokens (
  tenant_id    text NOT NULL,
  principal_id text NOT NULL,
  device_id    text NOT NULL,
  platform     text NOT NULL,          -- apns | fcm
  environment  text NOT NULL,          -- production | sandbox (fcm: production)
  token        text NOT NULL,
  updated_at   timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, principal_id, device_id)
);
CREATE UNIQUE INDEX push_tokens_token ON notify.push_tokens (platform, token);
```

Đăng ký là một transaction: `DELETE ... WHERE platform = ? AND token = ?` (thiết bị cũ giữ token này) rồi upsert theo khoá chính. Migration bằng Flyway như account-service.

### 5.3 Thu hồi

Consumer `gw.control`, group `notify-revocations` (một group chung cho mọi pod, vì việc xoá token chỉ cần làm một lần), lần đầu đọc từ đầu topic (topic compact):
- `Revoke{device}` → xoá token theo `(principal_id, device_id)` trên mọi tenant. `Revoke` không mang tenant, nhưng `device_id` là ULID không bao giờ dùng lại (CH2 §2), nên cặp này chỉ trỏ tới một thiết bị.
- `Revoke{principal_id}` → xoá mọi token của principal.
- `jti`, `client_id` → bỏ qua.

Thu hồi tới trước đăng ký (đăng ký bằng token đã bị thu hồi) bị chặn ở §5.1. Thu hồi tới sau thì xoá. Hai đường cùng nhau giữ bất biến 2.

### 5.4 Gửi

Consumer `c1.notify.requests`, group `notify`:

1. Kiểm `NotifyRules`; sai thì bỏ, có metric. Quá `expires_at_ms` thì bỏ.
2. **Dedup:** `SET notifydedup:{request_id} NX EX 3600` trên Redis; đã có thì bỏ (bất biến 4). Redis lỗi thì vẫn gửi (thà trùng hơn mất; `collapse_id` giúp máy không hiện hai dòng).
3. Tra token theo `(tenant, principal)` hoặc theo thiết bị. Không có token thì xong (metric `notify.no_token`).
4. Gửi song song tới từng token, theo tenant: dùng khoá Apple/Google của tenant trong cấu hình (`notify.tenants.<tenant>.apns|fcm`). Tenant không có cấu hình cho nền tảng đó thì bỏ, có metric.
5. Kết quả theo từng token:

| Kết quả | APNs | FCM | Xử lý |
|---|---|---|---|
| Thành công | 200 | 200 | metric |
| Token chết | 410; 400 `BadDeviceToken`, `DeviceTokenNotForTopic` | 404 `UNREGISTERED`; 400 `INVALID_ARGUMENT` về token | xoá token (chỉ khi token trong DB vẫn là token vừa gửi) |
| Tạm thời | 429, 500, 503, lỗi mạng | 429, 500, 503, lỗi mạng | thử lại tối đa 3 lần, chờ `Retry-After` hoặc 1 s / 4 s, không quá `expires_at_ms` |
| Lỗi cấu hình | 403 (khoá sai) | 401/403 | không thử lại, log lỗi, metric `notify.provider.auth_failed` |

6. Commit offset theo thứ tự, như các consumer khác của nền tảng: một yêu cầu xong (gửi xong, bỏ, hoặc hết lần thử) thì mới tính. Gửi song song nhiều yêu cầu trong một batch; giới hạn số request HTTP đang bay mỗi pod (`notify.max-in-flight`, mặc định 1 000).

### 5.5 Gửi tới Apple và Google

- **APNs:** HTTP/2 tới `api.push.apple.com` (sandbox: `api.sandbox.push.apple.com`), xác thực bằng khoá `.p8` (JWT ES256, `kid`, `iss = teamId`, làm mới mỗi 50 phút), header `apns-topic = bundleId`, `apns-push-type = alert`, `apns-priority`, `apns-expiration`, `apns-collapse-id`; thân `{"aps": {"alert": {...}, "badge": n, "thread-id": ..., "sound": "default"}, ...data}`. `badge = -1` thì bỏ trường `badge`.
- **FCM:** HTTP v1 `POST https://fcm.googleapis.com/v1/projects/<project>/messages:send`, OAuth2 bằng service account (access token cache tới gần hết hạn); `message.token`, `notification{title, body}` hoặc `android.notification{body_loc_key, body_loc_args}`, `android.notification.tag = thread_id`, `android.collapse_key = collapse_id`, `android.priority`, `android.ttl`, `android.notification.notification_count = badge` khi `badge >= 0`, `data`.
- Base URL của cả hai cấu hình được, để devenv và test trỏ sang bản giả lập (§7).

---

## 6. Lỗi và vận hành

| Tình huống | Hành vi |
|---|---|
| notify-service ngừng | Yêu cầu chờ trên Kafka (giữ 1 ngày); khi chạy lại, yêu cầu đã quá `expires_at_ms` (1 giờ) bị bỏ |
| chat-service ngừng | `push.offline` chờ (giữ 7 ngày); tin cũ hơn 1 giờ bị bỏ ở §3.1 |
| PostgreSQL của notify lỗi | API đăng ký trả `503`; consumer dừng commit và thử lại với lùi dần |
| APNs/FCM chậm | `max-in-flight` chặn; consumer lag tăng, có metric |
| Một người có nhiều điện thoại | Mỗi thiết bị một thông báo; thiết bị nào đang online thì người đó đã không offline (N1) |

**Metric** (Micrometer, như các service khác): `notify.requests{result}` (sent / no_token / expired / duplicate / invalid / no_config), `notify.sends{platform, result}`, `notify.tokens.removed{reason}` (provider / revoked / replaced / idle), `notify.send.latency{platform}`, consumer lag; phía chat: `chat.notify.offline{result}` (sent / muted / stale / not_chat), `chat.notify.channel.jobs{result}`, `chat.notify.channel.sent`.

---

## 7. Devenv, devtools và kiểm thử

- **Devenv:** service `notify-service` (Postgres schema `notify`, cùng Redis/Kafka); `kafka-init` tạo `c1.notify.requests` (32) và `chat.channel.notify` (16); `fake-apns` và `fake-fcm` chạy từ image devtools.
- **gateway-devtools:** lệnh `fake-apns` và `fake-fcm`: máy chủ HTTP (h2c cho APNs giả) nhận đúng dạng request của Apple/Google, ghi lại từng thông báo, trả lỗi theo token cấu hình được (ví dụ token có tiền tố `dead` → 410 / `UNREGISTERED`), và có `GET /received?token=` để test đọc lại. Lệnh `notify register --token ... --platform ...` gọi API §5.1.
- **Smoke:** bước `CHAT notify` như tiêu chí thành công.
- **Unit:** soạn nội dung (§3.5, cắt 200 ký tự, ẩn xem trước, nhắc tên trong hội thoại đã tắt); badge; quy tắc tắt; bảng kết quả §5.4; làm mới JWT APNs / OAuth FCM; `NotifyRules`.
- **IT (Testcontainers):** đăng ký → gửi → APNs/FCM giả nhận; token chết bị xoá; `Revoke{device}`/`Revoke{principal}` trên `gw.control` xoá token; cùng token đăng ký cho thiết bị khác thì thiết bị cũ mất token; đọc lại `NotifyRequest` không gửi hai lần; job kênh dừng giữa chừng rồi tiếp tục từ điểm dừng; bài mới thay job cũ.
- **E2E (chat-service, gateway thật):** người nhận online không có thông báo; offline có; đang tắt không có, được nhắc tên thì có.
- **Đo:** kênh 100 000 người theo dõi offline, APNs giả: thời gian tới thông báo cuối, ghi vào README.

---

## 8. Thứ tự làm (gợi ý cho plan)

1. Contracts: `notify.proto`, `AccountState.display_name`, testkit, topic devenv.
2. notify-service: khung, Postgres, API đăng ký + xác thực, thu hồi từ `gw.control`.
3. notify-service: consumer, dedup, APNs/FCM client, bảng kết quả; devtools `fake-apns`/`fake-fcm`.
4. account-service phát tên hiển thị; chat-service lưu `display_name`.
5. chat-service: mute, `hidePreview`, `OfflineNotifier`, soạn nội dung, badge.
6. chat-service: `ChannelNotifier`.
7. Smoke, E2E, đo, tài liệu (ADR, README, CHANGELOG).

---

## 9. Quyết định của người viết (chủ dự án chưa xét riêng)

1. **Người được nhắc tên vẫn nhận thông báo trong hội thoại đã tắt** (như Telegram). Rẻ, vì `ChatMessage.mentions` đã có.
2. **Thông báo kênh không đổi badge** (`badge = -1`): tính badge cho từng người theo dõi là một lượt đọc Scylla mỗi người, nhân với hàng triệu người mỗi bài.
3. **Không gỡ thông báo / cập nhật badge khi đọc ở thiết bị khác** trong CH4 (đã ghi ở "Ngoài phạm vi").
4. **Tin cũ hơn 1 giờ không đẩy**, và thông báo hết hạn sau 1 giờ.
5. **Không gộp nhiều tin thành một thông báo phía server** (không "5 tin mới"); máy tự gộp theo `thread_id`.
6. **Văn bản "Tin nhắn mới" do app dịch** (`loc_key`), vì server không biết ngôn ngữ của máy.
7. **Lưu token ở PostgreSQL** (như auth và account), không ở Scylla: notify-service là service nền tảng, và số dòng (một dòng mỗi điện thoại) vừa với Postgres.
8. **Một `NotifyRequest` cho mỗi người nhận**, kể cả với kênh: đơn giản, key Kafka theo người nhận, dedup theo người.
9. **Đi qua Kafka chứ không gọi gRPC** giữa chat-service và notify-service: notify ngừng thì yêu cầu chờ, không làm chậm chat.
10. **Gửi APNs/FCM bằng HTTP client của JDK** (HTTP/2), không dùng SDK của Apple/Google: ít phụ thuộc, dễ trỏ sang bản giả lập.
11. **Con số mặc định:** 5 000 thông báo kênh/s mỗi pod, `max-in-flight` 1 000, 3 lần thử, dọn token sau 180 ngày, trang 1 000 người theo dõi.
