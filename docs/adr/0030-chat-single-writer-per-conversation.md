# ADR 0030: Một nơi ghi mỗi hội thoại; `convSeq` cấp bằng LWT làm hàng rào

## Trạng thái

Accepted, 2026-11-12.

## Bối cảnh

Tin trong một hội thoại cần một thứ tự duy nhất (`convSeq` 1, 2, 3…) để client đồng bộ theo con trỏ và phát hiện lỗ hổng. Có ba hướng: cấp số trong Redis (`INCR`), cấp số bằng LWT cho mỗi tin, hoặc cho mỗi hội thoại một nơi ghi duy nhất. Redis có thể mất số khi failover. LWT cho mỗi tin quá đắt khi hội thoại nóng.

## Quyết định

- Mọi lệnh gửi đi qua `chat.commands`, key là `conversationId`. Vì vậy mọi lệnh của một hội thoại nằm trong một partition và chỉ một sequencer (consumer group `chat-sequencer`) xử lý chúng tại một thời điểm.
- Sequencer gom các lệnh của mỗi hội thoại trong một lần poll, rồi giữ chỗ cả lô bằng một LWT: `UPDATE conversations SET last_seq = known + n IF last_seq = known`. `known` lấy từ bộ nhớ, thiếu thì đọc ScyllaDB.
- LWT thất bại nghĩa là có một nơi ghi khác (rebalance). Khi đó sequencer huỷ lô, quên `known` và `seek` partition về đầu lô.
- Chống trùng theo `(conv_id, sender_id, client_msg_id)`, tra Redis trước rồi tới ScyllaDB (TTL 24 h). Lệnh trùng dùng lại `msgId` / `convSeq` cũ và phát lại sự kiện.
- Commit offset sau khi đã ghi tin, ghi sự kiện `chat.events` và trả kết quả HTTP.

## Hệ quả

- Không có `convSeq` nào bị dùng cho hai tin, kể cả khi hai sequencer chồng nhau lúc rebalance (`SequencerIT.twoSequencersNeverReuseASeq`).
- Crash giữa lúc ghi tin và lúc commit không sinh tin mới. Khi chạy lại, dedup tìm thấy tin cũ và phát lại sự kiện; client dedup theo `ChatMessage.msg_id`.
- Một hội thoại nóng bị giới hạn bởi một partition. Giới hạn 50 tin/giây (burst 100) mỗi hội thoại giữ nó dưới ngưỡng đó.
- Keyspace phải tắt tablets (`tablets = {'enabled': false}`), vì ScyllaDB 6.x chưa hỗ trợ LWT trên tablets.

## Nguồn

Spec chat-core §4.4, §8.
